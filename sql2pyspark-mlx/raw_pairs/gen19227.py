from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id")

with_windows = (
    df_order_items
    .withColumn("order_id_count_quantity", F.count("quantity").over(w))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w2))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("order_id_count_quantity") >= 13, "LARGE").otherwise("SMALL").alias("order_id_count_quantity_tier"),
        F.when(F.col("unit_price") >= 420, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
        "order_id",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("order_id_count_quantity_tier").desc())
)
