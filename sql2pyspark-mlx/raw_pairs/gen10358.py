from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-10-05")
    .select(
        "started_at",
        "browser",
        "country",
        F.to_date("started_at").alias("started_at_date"),
    )
    .orderBy("country")
)
