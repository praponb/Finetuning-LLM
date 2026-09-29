from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) & (F.col("quantity") != 3) & F.col("quantity").between(4, 8))
    .select("id", "unit_price")
)
