from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([14, 28, 31]) & F.col("customer_id").isin([17, 21, 23]))
    .groupBy("customer_id", "channel")
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
    )
    .filter(F.col("store_amount") > 42)
)
