from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.initcap(F.trim("device")).alias("device_title"),
        F.date_trunc("year", "started_at").alias("started_at_period"),
    )
)
