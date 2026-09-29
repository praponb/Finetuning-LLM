from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("order_id") == 34))
    .select("product_id")
    .orderBy("product_id")
)
