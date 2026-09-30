from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.count("*").alias("num_subscriptions"))
    .filter(F.col("num_subscriptions") < 1)
    .orderBy("plan")
)
