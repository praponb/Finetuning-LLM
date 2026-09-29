from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([16, 24, 31]) & ((F.col("order_id") == 5) | F.col("quantity").between(6, 7)))
    .select("id", "unit_price", "order_id", "product_id")
)
