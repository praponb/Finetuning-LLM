from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
    .groupBy("customer_id", F.year("start_date").alias("start_year"))
    .agg(F.count("*").alias("num_subscriptions"))
    .filter(F.col("num_subscriptions") < 4)
    .select("customer_id", "start_year", "num_subscriptions")
)
