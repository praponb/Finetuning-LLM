from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("store_id").orderBy(F.col("running_quantity").asc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-08-20"))) & F.col("quantity").between(75, 127))
    .select(
        "id",
        "store_id",
        "quantity",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)

with_windows = (
    filtered
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock_plus_months",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
    )
)

enriched = (
    with_windows
    .withColumn("running_count_quantity", F.count("quantity").over(w2))
    .withColumn("running_min_quantity", F.min("quantity").over(w2))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w2))
    .withColumn("first_known_quantity_avg_6_prior", F.avg("first_known_quantity").over(w3))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "first_known_quantity",
        "running_count_quantity",
        "running_min_quantity",
        "running_avg_quantity",
        "first_known_quantity_avg_6_prior",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("first_known_quantity") >= 226, "LARGE").otherwise("SMALL").alias("first_known_quantity_tier"),
        F.when(F.col("running_min_quantity") >= 256, "LARGE").otherwise("SMALL").alias("running_min_quantity_tier"),
        F.when(F.col("running_count_quantity") >= 14, "LARGE").otherwise("SMALL").alias("running_count_quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("running_min_quantity", 0.5).alias("running_min_quantity_p50"),
        F.count("*").alias("cnt_2"),
    )
    .filter(F.col("cnt_2") > 1)
    .orderBy(F.col("cnt_2").asc_nulls_first())
)
