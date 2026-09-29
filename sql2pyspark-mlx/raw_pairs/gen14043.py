from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([12, 17, 36]))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
    .withColumn("line_total_2", (F.col("unit_price") + F.col("quantity")))
)

result = (
    base
    .filter(F.col("unit_price") <= 260)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total_2",
        F.count("quantity").over(w).alias("last7_count_quantity"),
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
        F.max("quantity").over(w).alias("last7_max_quantity"),
    )
)
