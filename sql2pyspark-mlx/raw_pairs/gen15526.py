from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("unit_price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_avg_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price_whole").desc_nulls_first())
w_running = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy("running_sum_quantity", "id")
w_recent = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

enriched = (
    filtered
    .filter(F.col("order_id").isin([4, 12, 32]))
    .withColumn("running_min_quantity", F.min("quantity").over(w_part))
    .withColumn("running_max_quantity", F.max("quantity").over(w_part))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w_part))
    .withColumn("running_sum_quantity", F.sum("quantity").over(w_part))
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("running_avg_quantity", ignorenulls=True).over(w2).alias("last_known_running_avg_quantity"),
        F.dense_rank().over(w3).alias("unit_price_whole_dense_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.running_max_quantity",
        "src.running_avg_quantity",
        "src.running_sum_quantity",
        "src.unit_price_whole_dense_rank",
        F.col("b.status"),
        F.col("b.amount"),
        F.col("b.order_date"),
    )
)

joined_order_items = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.quantity",
        "src.running_max_quantity",
        "src.running_sum_quantity",
        "src.amount",
        F.col("o.amount").alias("order_amount"),
        F.col("o.shipping_fee"),
    )
)

metrics = (
    joined_order_items
    .select(
        "id",
        "quantity",
        "running_max_quantity",
        "running_sum_quantity",
        "amount",
        "order_amount",
        F.count("running_max_quantity").over(w_running).alias("order_id_count_running_max_quantity"),
        F.ntile(5).over(w5).alias("running_sum_quantity_bucket"),
        F.sum("quantity").over(w_recent).alias("running_quantity"),
    )
)

base = (
    metrics
    .filter(F.col("running_max_quantity").between(3.4, 4.6))
    .select(
        "id",
        "quantity",
        "running_sum_quantity",
        "amount",
        "order_amount",
        "running_sum_quantity_bucket",
        "running_quantity",
        F.round("quantity", 1).alias("quantity_rounded"),
        (F.col("running_sum_quantity") - F.col("running_sum_quantity_bucket")).alias("running_sum_quantity_minus_running_sum_quantity_bucket"),
    )
)

report = (
    base
    .select(
        "id",
        "quantity",
        "running_sum_quantity",
        "order_amount",
        "running_sum_quantity_bucket",
        "quantity_rounded",
        "running_sum_quantity_minus_running_sum_quantity_bucket",
    )
    .orderBy(F.col("quantity_rounded").asc_nulls_last(), "id")
)

result = (
    report
    .select(
        "id",
        "running_sum_quantity",
        "order_amount",
        "running_sum_quantity_bucket",
        "quantity_rounded",
        "running_sum_quantity_minus_running_sum_quantity_bucket",
    )
)
