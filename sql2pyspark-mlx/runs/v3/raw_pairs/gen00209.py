from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("customer_id") == 15) & F.month("start_date").isin([5, 7, 11]))
    .withColumn("status_upper", F.upper("status"))
)

agg = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    agg
    .orderBy("plan")
    .limit(10)
)
