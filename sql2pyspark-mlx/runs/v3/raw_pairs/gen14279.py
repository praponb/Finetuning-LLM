from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") < 40)
    .orderBy(F.col("product_id").desc())
)
