from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_claimed_amount_ru = Window.partitionBy("product").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_product_by_claimed_amount = Window.partitionBy("product").orderBy("claimed_amount", "id")
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")

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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

metrics = (
    in_window
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "approved_amount",
        "premium",
        F.first("claimed_amount", ignorenulls=True).over(w_product_by_claimed_amount_ru).alias("first_known_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_policy_id"),
        F.dense_rank().over(w_all_by_claimed_amount).alias("claimed_amount_dense_rank"),
        F.dense_rank().over(w_product_by_claimed_amount).alias("claimed_amount_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "first_known_claimed_amount",
        "approved_amount_share_of_claim_type_policy_id",
        "claimed_amount_dense_rank",
        "claimed_amount_dense_rank_2",
        F.abs(F.col("approved_amount") - 15477.7).alias("approved_amount_distance"),
    )
    .orderBy(
        F.col("claimed_amount_dense_rank_2").desc(),
        F.col("approved_amount_share_of_claim_type_policy_id").desc(),
    )
)
