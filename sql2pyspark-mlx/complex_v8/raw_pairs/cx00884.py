from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_claim_type = Window.partitionBy("claim_type")
w_product_claim_type_by_premiu = Window.partitionBy("product", "claim_type").orderBy("premium", "id")
w_product_policy_id_by_premium = Window.partitionBy("product", "policy_id").orderBy("premium", "id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")

joined_ranges = (
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

metrics = (
    joined_ranges
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
        F.min("premium").over(w_claim_type).alias("claim_type_min_premium"),
        F.round(F.cume_dist().over(w_product_claim_type_by_premiu), 3).alias("premium_cume_dist"),
        F.rank().over(w_product_policy_id_by_premium).alias("premium_rank"),
        F.round(F.cume_dist().over(w_all_by_approved_amount), 3).alias("approved_amount_cume_dist_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "claim_type",
        "premium",
        "approved_amount_cume_dist",
        "claim_type_min_premium",
        "premium_cume_dist",
        "premium_rank",
        "approved_amount_cume_dist_2",
        (F.col("approved_amount") + F.col("premium")).alias("approved_amount_plus_premium"),
    )
)
