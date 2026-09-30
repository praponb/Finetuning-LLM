from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "device",
        F.trim("browser").alias("clean_browser"),
        F.date_trunc("month", "started_at").alias("started_at_period"),
    )
    .orderBy(F.col("clean_browser").desc())
)
