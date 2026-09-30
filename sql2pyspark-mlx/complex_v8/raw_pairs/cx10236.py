from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_approved = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id")
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

windowed = (
    in_window
    .select(
        "*",
        F.rank().over(w_claim_date_year_by_approved).alias("approved_amount_rank"),
        F.size(F.collect_set("product").over(w_claim_type_policy_id)).alias("distinct_product_in_claim_type_policy_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "premium",
        "approved_amount_rank",
        "distinct_product_in_claim_type_policy_id",
        F.abs(F.col("approved_amount_rank") - 5).alias("approved_amount_rank_distance"),
    )
    .orderBy(
        F.col("approved_amount_rank_distance").desc(),
        F.col("premium").asc_nulls_last(),
    )
)
