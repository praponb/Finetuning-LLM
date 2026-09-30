from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .orderBy(F.col("cancelled_count").desc())
)
