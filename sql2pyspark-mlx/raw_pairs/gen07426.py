from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"))
    .intersect(
        df_transactions
        .filter(F.col("txn_time") >= "2023-08-01")
        .select(F.col("amount").alias("value"))
    )
)
