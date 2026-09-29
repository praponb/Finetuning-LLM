from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"))
    .orderBy("basic_count")
)
