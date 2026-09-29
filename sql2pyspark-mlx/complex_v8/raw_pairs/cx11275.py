from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status = Window.partitionBy("product", "status")
w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_status_by_start_date_moving = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-1, Window.currentRow)

filtered = (
    df_policies
    .filter((F.col("holder_id") <= 9) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.avg("premium").over(w_product_status), 2).alias("product_status_avg_premium"),
        F.round(F.cume_dist().over(w_status_by_start_date), 3).alias("start_date_cume_dist"),
        F.max("premium").over(w_status_by_start_date_moving).alias("premium_max_2"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_status_avg_premium",
        "s.start_date_cume_dist",
        "s.premium_max_2",
        F.col("p.segment").alias("holder_segment"),
    )
)

labeled = (
    with_parent
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "product_status_avg_premium",
        "premium_max_2",
        "holder_segment",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.round(F.col("premium_max_2") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_max_2_per_premium"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

profiled = (
    labeled.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.max_claim_approved_amount",
    )
)

result = (
    profiled
    .filter(F.col("status_clean").isNotNull())
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "premium_max_2",
        "status_clean",
        "premium_max_2_per_premium",
        "claim_count",
    )
    .orderBy(F.col("claim_count").asc(), F.col("start_date").asc_nulls_last(), "id")
    .limit(20)
)
