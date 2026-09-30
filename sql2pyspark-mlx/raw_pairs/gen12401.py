from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("order_id") == 17))
    .select("id", "product_id")
)
