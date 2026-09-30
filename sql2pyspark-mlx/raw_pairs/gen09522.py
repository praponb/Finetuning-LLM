from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("order_item_quantity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("order_item_unit_price").desc(), "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("order_item_unit_price").desc(), "id")
w6 = Window.orderBy(F.col("order_item_unit_price").desc_nulls_first())
w7 = Window.partitionBy("status", "customer_id").orderBy(F.col("order_item_unit_price_bucket").asc_nulls_last(), "id")

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "customer_id",
        "status",
        "order_item_unit_price",
        F.avg("order_item_quantity").over(w2).alias("order_item_quantity_avg_5"),
        F.ntile(5).over(w3).alias("order_item_unit_price_bucket"),
        F.round((F.col("order_item_quantity") / F.sum("order_item_quantity").over(w4)), 4).alias("order_item_quantity_share"),
        F.ntile(5).over(w5).alias("order_item_unit_price_bucket_2"),
        F.percent_rank().over(w6).alias("order_item_unit_price_percent_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    ranked
    .filter(F.col("order_item_unit_price_percent_rank") > 0.58)
    .select(
        "id",
        "customer_id",
        "order_item_unit_price",
        "order_item_unit_price_bucket",
        "order_item_quantity_share",
        "order_item_unit_price_bucket_2",
        "order_item_unit_price_percent_rank",
        F.col("row_num").cast("double").alias("row_num_str"),
    )
)

result = (
    cleaned
    .withColumn("order_item_unit_price_tier", F.when(F.col("order_item_unit_price") >= 320, "HIGH").otherwise("LOW"))
    .cube("order_item_unit_price_tier", "customer_id")
    .agg(
        F.avg("order_item_unit_price_bucket_2").alias("avg_order_item_unit_price_bucket_2"),
        F.round(F.avg("order_item_quantity_share"), 2).alias("avg_order_item_quantity_share"),
        F.min("row_num_str").alias("min_row_num_str"),
        F.percentile_approx("order_item_quantity_share", 0.5).alias("order_item_quantity_share_p50"),
        F.grouping("order_item_unit_price_tier").alias("grouping_level"),
    )
    .filter(F.col("min_row_num_str") < 1000000)
)
