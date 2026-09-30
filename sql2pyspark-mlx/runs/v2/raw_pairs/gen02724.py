from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 32]))
    .select("product_id", "quantity")
    .orderBy(F.col("product_id").desc())
)
