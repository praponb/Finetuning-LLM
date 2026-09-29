from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 990) & F.col("customer_id").isNull() & (F.col("shipping_fee") < 38))
    .groupBy("channel")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
    .orderBy("channel")
    .limit(1)
)
