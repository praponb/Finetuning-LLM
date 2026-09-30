from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc(), "id")
w_holder_id_product_by_premium_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc())
w_status = Window.partitionBy("status")
w_latest = Window.partitionBy("product").orderBy(F.col("end_date").desc(), F.col("id").desc())

scoped = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.ntile(3).over(w_holder_id_product_by_premium).alias("premium_tercile"),
        F.round(F.cume_dist().over(w_holder_id_product_by_premium_2), 3).alias("premium_cume_dist"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
    )
)

unique_rows = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    unique_rows
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.abs(F.col("premium_cume_dist") - 0.3).alias("premium_cume_dist_distance"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

line_level = (
    labeled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "PAID"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.premium_tercile",
        "s.premium_vs_status_avg",
        "s.product_slug",
        "s.premium_cume_dist_distance",
        "s.premium_adj",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

qualified = (
    line_level.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2025-04-14").cast("date")) & (F.col("c.status") != "IN_REVIEW"), "left_anti")
    .filter(F.col("holder_id") >= 9)
)

enriched_claim = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.*",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

result = (
    enriched_claim
    .select(
        "id",
        "product",
        "premium",
        "premium_tercile",
        "premium_cume_dist_distance",
        "premium_adj",
        "claim_status",
        "claim_reported_at",
        "claim_approved_amount",
        "claim_claimed_amount",
        "claim_type",
        "claim_reported_at_2",
        F.abs(F.col("claim_approved_amount") - 14217.6).alias("claim_approved_amount_distance"),
    )
    .orderBy(F.col("premium_adj").desc(), F.col("claim_claimed_amount").desc())
)
