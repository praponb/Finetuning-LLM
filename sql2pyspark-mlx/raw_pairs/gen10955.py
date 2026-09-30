from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.date_trunc("month", "started_at").alias("started_at_period"),
    )
    .orderBy("browser")
)
