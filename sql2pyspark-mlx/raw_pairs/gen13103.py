from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([20, 22]) | F.col("quantity").between(4, 6))
    .select("id", "quantity")
)
