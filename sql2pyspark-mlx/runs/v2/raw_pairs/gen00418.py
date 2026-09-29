from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .groupBy("order_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 4.3)
    .select("order_id", "min_quantity")
    .orderBy(F.col("min_quantity").desc())
)
