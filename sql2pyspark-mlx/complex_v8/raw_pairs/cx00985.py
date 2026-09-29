from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2024) | (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

per_group = (
    cleaned
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "product",
        F.date_trunc("month", "end_date").alias("end_date_month"),
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("product") == "TRAVEL", F.col("premium")).otherwise(0)).alias("premium_product_travel"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    per_group
    .select(
        "start_date_ym",
        "product",
        "product_list",
        "premium_product_travel",
    )
    .orderBy(F.col("start_date_ym").desc(), "premium_product_travel")
)
