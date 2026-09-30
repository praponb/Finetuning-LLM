from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube("status", "plan")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.min("start_date").alias("min_start_date"),
    )
)
