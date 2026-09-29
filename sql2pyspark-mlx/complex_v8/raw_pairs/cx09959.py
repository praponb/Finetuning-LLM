from pyspark.sql import functions as F

valid = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date_2"),
    )
)

stats = (
    valid
    .groupBy("status", F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.max("premium").alias("max_premium_check"),
    )
    .filter(F.col("max_premium_check") > 1051.9)
    .drop("max_premium_check")
)

result = (
    stats
    .select(
        "status",
        "product_list",
        F.concat(F.col("end_date_ym"), F.lit("#"), F.col("row_count").cast("string")).alias("end_date_ym_ref"),
    )
    .orderBy(F.col("end_date_ym_ref").desc(), F.col("status").asc())
)
