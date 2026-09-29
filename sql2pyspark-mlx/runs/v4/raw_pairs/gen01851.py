from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("line_total", "id").rowsBetween(-3, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

base = (
    df_order_items
    .filter(F.col("unit_price") <= 230)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.ceil("quantity").alias("quantity_whole"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.launch_date"))
)

enriched_order_items = (
    enriched
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
    .withColumn("unit_price_avg_3", F.avg("unit_price").over(w3))
)

result = (
    enriched_order_items
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_whole",
        "line_total",
        "launch_date",
        "quantity_bucket",
        "cumulative_quantity",
        "unit_price_avg_3",
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("line_total").desc())
)
