from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity_plus_line_total_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(-6, -1)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total_2"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("line_total") > 62)
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

base = (
    cleaned_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("quantity") + F.col("line_total_2")).alias("quantity_plus_line_total_2"),
        F.floor("line_total").alias("line_total_whole"),
    )
)

with_windows = (
    base
    .withColumn("last_known_line_total_whole", F.last("line_total_whole", ignorenulls=True).over(w))
    .withColumn("quantity_plus_line_total_2_avg_6_prior", F.avg("quantity_plus_line_total_2").over(w2))
)

filtered = (
    with_windows
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "line_total",
        "quantity_rounded",
        "line_total_whole",
        "last_known_line_total_whole",
        "quantity_plus_line_total_2_avg_6_prior",
        F.round((F.col("line_total_2") * 1.15), 2).alias("line_total_2_adjusted"),
    )
)

stats = (
    filtered
    .groupBy("product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_rounded", 0.95).alias("quantity_rounded_p95"),
        F.avg("last_known_line_total_whole").alias("avg_last_known_line_total_whole"),
        F.percentile_approx("line_total_whole", 0.9).alias("line_total_whole_p90"),
    )
)

result = (
    stats
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "distinct_ids",
        "quantity_rounded_p95",
        "avg_last_known_line_total_whole",
        "line_total_whole_p90",
        F.col("max_inventory_quantity"),
    )
)
