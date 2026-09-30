from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)
