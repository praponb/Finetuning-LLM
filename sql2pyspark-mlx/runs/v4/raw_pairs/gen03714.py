from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "PENDING"]) & F.col("store_id").isin([5, 28]))
)

summary = (
    base
    .groupBy("channel")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    summary
    .filter(F.col("max_amount") < 1240)
    .select("channel", "max_amount")
    .orderBy(F.col("max_amount").desc(), F.col("channel").desc())
)
