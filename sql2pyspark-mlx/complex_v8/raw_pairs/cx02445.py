from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("end_date") >= F.lit("2026-01-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
    )
)

stats = (
    cleaned
    .groupBy(
        "holder_id",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "product",
    )
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.count("*").alias("cnt"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.min("premium_adj").alias("min_premium_adj"),
        F.min("premium_filled").alias("min_premium_filled"),
        F.min("premium").alias("min_premium"),
        F.sum("premium_distance").alias("sum_premium_distance_check"),
    )
    .filter(F.col("sum_premium_distance_check") > 750.0)
    .drop("sum_premium_distance_check")
)

with_details_holder = (
    stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.holder_id",
        "s.start_date_ym",
        "s.product",
        "s.distinct_holder_ids",
        "s.start_date_span_days",
        "s.min_premium",
        "p.holder_name",
    )
)

result = (
    with_details_holder
    .select(
        "holder_id",
        "start_date_ym",
        "product",
        "distinct_holder_ids",
        "start_date_span_days",
        "min_premium",
        "holder_name",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)
