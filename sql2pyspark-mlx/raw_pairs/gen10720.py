from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w_part = Window.orderBy(F.col("greatest_quantity_unit_price").desc(), "id")
w_part_2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("unit_price", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("order_id")
w6 = Window.orderBy("cumulative_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("greatest_quantity_unit_price", "id")
w8 = Window.orderBy(F.col("unit_price").desc())
w_ordered = Window.orderBy("quantity_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

with_windows = (
    valid
    .filter(F.col("unit_price") <= 170)
    .withColumn("unit_price_bucket", F.ntile(10).over(w))
    .withColumn("greatest_quantity_unit_price_bucket", F.ntile(10).over(w_part))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_part_2))
)

metrics = (
    with_windows
    .select(
        "id",
        "quantity",
        "unit_price",
        "greatest_quantity_unit_price",
        "cumulative_quantity",
        F.avg("quantity").over(w4).alias("quantity_avg_2_prior"),
        F.sum("quantity").over(w5).alias("order_id_sum_quantity"),
    )
)

with_windows_order_items = (
    metrics
    .withColumn("running_order_id_sum_quantity", F.sum("order_id_sum_quantity").over(w6))
    .withColumn("greatest_quantity_unit_price_bucket", F.ntile(4).over(w_running))
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w8))
    .withColumn("cumulative_order_id_sum_quantity", F.sum("order_id_sum_quantity").over(w_ordered))
)

base = (
    with_windows_order_items
    .filter(F.col("greatest_quantity_unit_price") >= 51)
    .select(
        "*",
        F.ceil("greatest_quantity_unit_price").alias("greatest_quantity_unit_price_whole"),
    )
)

filtered = (
    base
    .filter(F.col("quantity_avg_2_prior") < 5)
    .select(
        "*",
        F.col("order_id_sum_quantity").cast("string").alias("order_id_sum_quantity_num"),
    )
)

totals = (
    filtered
    .groupBy(
        F.when(F.col("quantity_avg_2_prior") >= 9, "LARGE").otherwise("SMALL").alias("quantity_avg_2_prior_tier"),
        F.when(F.col("greatest_quantity_unit_price_bucket") >= 8, "LARGE").otherwise("SMALL").alias("greatest_quantity_unit_price_bucket_tier"),
        F.when(F.col("cumulative_order_id_sum_quantity") >= 217, "LARGE").otherwise("SMALL").alias("cumulative_order_id_sum_quantity_tier"),
    )
    .agg(
        F.percentile_approx("running_order_id_sum_quantity", 0.95).alias("running_order_id_sum_quantity_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    totals
    .select(
        "quantity_avg_2_prior_tier",
        "greatest_quantity_unit_price_bucket_tier",
        "cumulative_order_id_sum_quantity_tier",
        "distinct_ids",
        "distinct_ids_2",
    )
    .orderBy(F.col("distinct_ids_2").desc_nulls_last())
)
