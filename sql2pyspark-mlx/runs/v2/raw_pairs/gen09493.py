from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium", "standard"]))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
    .orderBy(F.col("end_quarter").desc(), "premium_count")
    .limit(1)
)
