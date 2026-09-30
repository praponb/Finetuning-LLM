from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 4) & (F.col("quantity") == 5))
    .select("quantity", "order_id", "product_id")
    .orderBy("order_id", F.col("quantity").desc())
)
