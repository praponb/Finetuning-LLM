from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_claim_adjuster_id = Window.partitionBy("product", "claim_adjuster_id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id", "claim_id")

filtered_policys = (
    df_policies
    .filter(F.col("holder_id") >= 14)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
    )
)

detail_rows = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
        "c.claim_date",
    )
)

ranked = (
    detail_rows
    .withColumn("claim_claimed_amount_share_of_product_claim_adju", F.round(F.col("claim_claimed_amount") / F.nullif(F.sum("claim_claimed_amount").over(w_product_claim_adjuster_id), F.lit(0)), 4))
    .withColumn("days_since_prev_claim_date", F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)))
)

without_matches = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isNotNull()) & (F.col("c.reported_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("claim_claimed_amount_share_of_product_claim_adju").between(0.2, 0.9))
)

result = (
    without_matches
    .select(
        "id",
        "claim_id",
        F.col("claim_claimed_amount_share_of_product_claim_adju").alias("m_claim_claimed_amount_share_of_product_claim_ad"),
        F.col("claim_claimed_amount").alias("claim_claimed_amount_v"),
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["m_claim_claimed_amount_share_of_product_claim_ad", "claim_claimed_amount_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
