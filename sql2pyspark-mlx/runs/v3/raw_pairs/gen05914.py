from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 7) | F.col("quantity").between(3, 5))
    .select("order_id", "unit_price", "quantity")
    .orderBy(F.col("quantity").desc())
)
