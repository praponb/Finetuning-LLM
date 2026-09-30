from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("unit_price")
    .union(df_transactions.select("amount"))
)
