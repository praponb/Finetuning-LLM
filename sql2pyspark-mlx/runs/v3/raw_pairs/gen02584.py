from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        "browser",
        "device",
        F.date_trunc("MONTH", "started_at").alias("started_at_period"),
    )
    .orderBy("browser", "device", "id")
    .limit(1)
)
