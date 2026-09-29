from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "started_at",
        "country",
        F.upper("browser").alias("browser_upper"),
    )
    .orderBy(F.col("browser_upper").desc(), F.col("started_at").desc())
)
