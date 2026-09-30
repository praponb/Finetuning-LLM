from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-10-26").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_group", F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "Core").otherwise("Other"))
)

result = (
    cleaned
    .groupBy("status", "customer_id")
    .pivot("plan", ["enterprise", "premium"])
    .agg(F.sum("monthly_fee"))
)
