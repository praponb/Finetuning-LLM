from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & ((F.col("quantity") < 8) | F.col("order_id").isin([5, 15, 25, 37])))
    .select("product_id")
    .orderBy(F.col("product_id").desc())
)
