from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(180, 270))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
    .orderBy(F.col("quantity").desc(), "id")
    .limit(20)
)
