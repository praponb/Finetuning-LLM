from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy(F.col("unit_price").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([3, 6, 38]))
    .select(
        "*",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 100, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

result = (
    filtered
    .select(
        "id",
        "quantity",
        "quantity_tier",
        F.first("quantity", ignorenulls=True).over(w).alias("first_known_quantity"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_2"),
        F.ntile(3).over(w_part).alias("unit_price_bucket"),
    )
)
