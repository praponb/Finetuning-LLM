from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("holder_id") >= 3)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label_2"),
    )
)

status_stats = (
    recent
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

above_average = (
    recent.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

rollup_base = (
    above_average
    .groupBy("status", "holder_id")
    .agg(
        F.round(F.sum("premium_minus_median") / F.nullif(F.sum("product_len"), F.lit(0)), 3).alias("premium_minus_median_to_product_len_ratio"),
        F.max("end_date").alias("last_end_date"),
        F.min("premium_minus_median").alias("min_premium_minus_median"),
        F.count("*").alias("cnt"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    rollup_base
    .groupBy("cnt")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.sum("avg_premium"))
    .orderBy("cnt")
)
