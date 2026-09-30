from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 14, 21, 31]))
    .select(F.lit("order_item").alias("origin"), "unit_price")
    .union(df_transactions.select(F.lit("transaction").alias("origin"), "amount"))
    .orderBy("unit_price")
)
