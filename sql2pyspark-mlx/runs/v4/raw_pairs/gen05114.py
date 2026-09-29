from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 4) & F.col("order_id").isin([4, 15, 29]) & F.col("order_id").isin([8, 11, 26, 31]))
    .select("order_id", "quantity")
)
