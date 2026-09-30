from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.year("start_date") == 2024) & F.col("customer_id").isNotNull() & F.month("end_date").isin([3, 4, 12]))
)

summary = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
)

result = summary.filter(F.col("cnt") < 6)
