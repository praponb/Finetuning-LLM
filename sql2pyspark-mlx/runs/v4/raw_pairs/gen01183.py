from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
w3 = Window.orderBy("order_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("order_date", "id")
w5 = Window.partitionBy("channel").orderBy("amount", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-09").cast("date")) & F.col("customer_id").isin([2, 18, 21, 32]))
    .select(
        "*",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.add_months("order_date", 1).alias("order_date_plus_months"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

latest = (
    with_last
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.store_name"))
)

scored = (
    joined
    .withColumn("cumulative_order_item_quantity", F.sum("order_item_quantity").over(w3))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 2).over(w4))
    .withColumn("amount_bucket", F.ntile(10).over(w5))
)

with_last_orders = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

totals = (
    with_last_orders
    .groupBy(
        F.when(F.col("discount") >= 0.4, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.max(F.when(F.col("channel") == "web", F.col("cumulative_order_item_quantity"))).alias("web_max_cumulative_order_item_quantity"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .filter(F.col("avg_shipping_fee") < 0)
)

result = (
    totals
    .select(
        "discount_tier",
        "avg_shipping_fee",
        "web_max_cumulative_order_item_quantity",
        "distinct_store_ids",
    )
)
