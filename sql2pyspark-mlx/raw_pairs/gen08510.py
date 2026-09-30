from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id", "status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("order_date"), "status").orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("channel").orderBy("amount_dense_rank", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy(F.col("running_sum_amount").asc_nulls_last())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("payment_method", "channel").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("channel", "payment_method").orderBy("running_sum_amount", "id")

cleaned = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.concat("status", F.lit(" - "), "channel").alias("status_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.min("amount").over(w_ordered).alias("running_min_amount"),
        F.count("amount").over(w_ordered).alias("running_count_amount"),
        F.sum("amount").over(w_ordered).alias("running_sum_amount"),
        F.avg("amount").over(w_ordered).alias("running_avg_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.resolved_channel",
        "s.running_min_amount",
        "s.running_sum_amount",
        "s.amount_dense_rank",
        F.col("c.method").alias("payment_method"),
        F.col("c.paid_at"),
    )
)

enriched_orders = (
    with_latest
    .filter(F.col("status") != "SHIPPED")
    .withColumn("last3_sum_amount_dense_rank", F.sum("amount_dense_rank").over(w4))
    .withColumn("last3_max_amount_dense_rank", F.max("amount_dense_rank").over(w4))
    .withColumn("last3_min_amount_dense_rank", F.min("amount_dense_rank").over(w4))
    .withColumn("last3_count_amount_dense_rank", F.count("amount_dense_rank").over(w4))
    .withColumn("running_sum_amount_percent_rank", F.percent_rank().over(w5))
)

with_latest_orders = (
    enriched_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

metrics = (
    with_latest_orders
    .withColumn("running_sum_amount_max_90d", F.max("running_sum_amount").over(w7))
    .withColumn("running_sum_amount_bucket", F.ntile(4).over(w8))
)

result = (
    metrics
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .groupingSets(
        [["region", "channel"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("src.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.running_sum_amount_percent_rank")).alias("min_running_sum_amount_percent_rank"),
    )
    .orderBy(F.col("min_running_sum_amount_percent_rank").desc_nulls_last())
)
