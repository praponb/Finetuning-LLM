from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first())
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee_rank").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("shipping_fee_rank", "id").rowsBetween(-2, -1)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.floor("quantity").alias("quantity_whole"),
        F.when(F.col("unit_price") >= 420, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.quantity",
        "e.unit_price",
        "e.unit_price_band",
        F.col("b.shipping_fee"),
    )
)

scored = (
    joined
    .filter(F.col("unit_price").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "shipping_fee",
        F.rank().over(w).alias("shipping_fee_rank"),
        F.percent_rank().over(w2).alias("unit_price_percent_rank"),
        F.corr("unit_price", "quantity").over(w3).alias("corr_unit_price_quantity"),
    )
)

scored_order_items = (
    scored
    .withColumn("shipping_fee_bucket", F.ntile(5).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("corr_unit_price_quantity_avg_2_prior", F.avg("corr_unit_price_quantity").over(w6))
)

cleaned = (
    scored_order_items
    .filter(F.col("corr_unit_price_quantity") < 0.6)
    .withColumn("unit_price_times_shipping_fee_bucket", (F.col("unit_price") * F.col("shipping_fee_bucket")))
)

result = (
    cleaned
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price",
        "shipping_fee",
        "shipping_fee_rank",
        "unit_price_percent_rank",
        "corr_unit_price_quantity",
        "shipping_fee_bucket",
        "event_seq",
        F.col("avg_order_item_unit_price"),
    )
)
