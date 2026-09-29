from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.month("order_date").isin([6, 7, 10]) & (F.col("customer_id") == 15) & F.col("amount").between(680, 730))
)

agg = (
    valid_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
)

result = (
    agg
    .orderBy("order_quarter")
    .limit(10)
)
