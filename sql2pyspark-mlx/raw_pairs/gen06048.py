from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("amount") < 1500)
    .withColumn("discount_minus_amount", (F.col("discount") - F.col("amount")))
)

summary = (
    base
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.round(F.avg("discount_minus_amount"), 2).alias("avg_discount_minus_amount"))
)

result = (
    summary
    .filter(F.col("avg_discount_minus_amount") < 31)
    .select("order_quarter", "avg_discount_minus_amount")
    .orderBy(F.col("avg_discount_minus_amount").desc(), "order_quarter")
)
