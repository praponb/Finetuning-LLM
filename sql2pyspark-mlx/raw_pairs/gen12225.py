from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w_running = Window.orderBy("supplier_name_length", "id").rowsBetween(-6, Window.currentRow)

ranked = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "supplier_name")
)

cleaned = (
    ranked
    .filter(F.col("supplier_name").isNotNull())
    .select("*", F.lower("supplier_name").alias("supplier_name_lower"))
)

valid = (
    cleaned
    .filter(F.col("supplier_name").like("To%"))
    .select(
        "*",
        F.lower("supplier_name").alias("supplier_name_lower_2"),
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.count("supplier_name_length").over(w_running).alias("last7_count_supplier_name_length"),
        F.max("supplier_name_length").over(w_running).alias("last7_max_supplier_name_length"),
        F.min("supplier_name_length").over(w_running).alias("last7_min_supplier_name_length"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("last7_max_supplier_name_length") >= 88, "PREMIUM").otherwise("STANDARD").alias("last7_max_supplier_name_length_tier"),
        F.when(F.col("last7_count_supplier_name_length") >= 14, "LARGE").otherwise("SMALL").alias("last7_count_supplier_name_length_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("last7_count_supplier_name_length").alias("max_last7_count_supplier_name_length"),
        F.min("last7_min_supplier_name_length").alias("min_last7_min_supplier_name_length"),
        F.percentile_approx("supplier_name_length", [0.25, 0.5, 0.75]).alias("supplier_name_length_quartiles"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
