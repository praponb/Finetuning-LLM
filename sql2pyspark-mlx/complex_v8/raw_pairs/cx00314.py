from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("status") == "CANCELLED") & (F.col("product") == "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("status").isin("ACTIVE", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

enriched = (
    recent_policys
    .filter(F.col("status_clean").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status_group",
        "status_clean",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

tagged = (
    enriched
    .filter(F.col("status_group") == "group_b")
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.concat_ws("-", F.col("product"), F.col("status_group")).alias("product_status_group_label"),
    )
)

scored = (
    tagged
    .filter(F.col("start_date") < F.lit("2025-05-30").cast("date"))
    .select(
        "*",
        (F.col("premium_filled") - F.col("days_since_start_date")).alias("premium_filled_minus_days_since_start_date"),
        F.round(F.col("premium_filled") * 1.07, 2).alias("premium_filled_adj"),
        (F.col("premium_filled") + F.col("days_since_start_date")).alias("premium_filled_plus_days_since_start_date"),
    )
)

result = (
    scored
    .groupBy("end_date")
    .pivot("product", ["HOME", "LIFE", "AUTO", "HEALTH"])
    .agg(F.avg("premium_filled").alias("avg_v"), F.sum("premium_filled").alias("sum_v"))
)
