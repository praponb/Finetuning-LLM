from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.max("start_date").alias("last_start_date"))
)
