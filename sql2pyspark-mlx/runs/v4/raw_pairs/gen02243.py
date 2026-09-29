from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "started_at",
        F.date_trunc("MONTH", "started_at").alias("started_at_period"),
    )
    .orderBy("started_at_period")
)
