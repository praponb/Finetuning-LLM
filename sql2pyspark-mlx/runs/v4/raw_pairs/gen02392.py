from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .unionByName(df_transactions.select(F.col("amount").alias("unit_price")))
    .distinct()
    .orderBy("unit_price")
)
