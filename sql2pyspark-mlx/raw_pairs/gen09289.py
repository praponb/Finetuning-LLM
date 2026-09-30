from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 5) & F.col("quantity").between(4, 6))
    .select("id", "unit_price", "product_id", "order_id")
)
