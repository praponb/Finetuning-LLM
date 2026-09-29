from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") >= 5) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
        F.date_add(F.col("end_date"), 7).alias("end_date_plus_7d"),
    )
)

enriched = (
    filtered_policys
    .select(
        "*",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

result = (
    enriched
    .cube("status", "holder_id")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.sum("premium_distance").alias("total_premium_distance"),
    )
)
