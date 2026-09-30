from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", F.upper("status").alias("status_upper"))
    .orderBy(F.col("status").desc())
)
