from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

enriched = (
    cleaned
    .withColumn("quantity_avg_2", F.avg("quantity").over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("running_unit_price", F.sum("unit_price").over(w3))
    .withColumn("next_unit_price", F.lead("unit_price", 1).over(w4))
)

enriched_order_items = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.shipping_fee"), F.col("b.channel"), F.col("b.amount"))
)

result = (
    enriched_order_items
    .groupBy(
        F.when(F.col("next_unit_price") >= 430, "LARGE").otherwise("SMALL").alias("next_unit_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("next_unit_price_tier").desc_nulls_last())
)
