from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("cnt").desc())
)
