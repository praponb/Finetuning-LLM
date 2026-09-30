from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_all_by_premium = Window.orderBy("premium", "id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_trends = (
    in_window
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "premium",
        F.size(F.collect_set("product").over(w_status_claim_type)).alias("distinct_product_in_status_claim_type"),
        F.lag("claimed_amount", 3, 0).over(w_all_by_premium).alias("prev3_claimed_amount"),
    )
)

result = (
    with_trends
    .filter(F.col("distinct_product_in_status_claim_type") > 3)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "premium",
        "distinct_product_in_status_claim_type",
        "prev3_claimed_amount",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.when(F.col("premium") >= 2945.9, "large").when(F.col("premium") >= 2289.0, "medium").otherwise("small").alias("premium_tier"),
    )
    .orderBy(F.col("claim_date").asc_nulls_last())
)
