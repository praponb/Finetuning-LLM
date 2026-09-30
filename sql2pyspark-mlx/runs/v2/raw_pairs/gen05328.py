from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([15, 24, 29]) | (F.col("quantity") != 7))
    .select("order_id", "quantity")
)
