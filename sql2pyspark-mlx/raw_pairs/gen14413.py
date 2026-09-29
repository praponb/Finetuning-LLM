from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
    )
)
