from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 6, 10, 40]) & F.col("order_id").isin([17, 28]))
    .select("product_id", "unit_price", "quantity")
)
