from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-08-03").cast("date")))
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

summary = (
    valid
    .filter(F.col("holder_id").isNotNull())
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.sum(F.when(F.col("product").isin("HEALTH", "LIFE"), 1).otherwise(0)).alias("n_product_in_health_life"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

result = (
    summary
    .select(
        "holder_id",
        "n_rows",
        "n_product_in_health_life",
        "distinct_ids",
        "distinct_status",
        (F.col("n_rows") - F.col("distinct_status")).alias("n_rows_minus_distinct_status"),
    )
    .orderBy("n_rows")
)
