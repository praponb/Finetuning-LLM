from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("order_id").isin([37, 38]))
    .select("id", "quantity", "product_id", "unit_price")
)
