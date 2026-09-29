from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-04-01")
    .select(
        "started_at",
        "browser",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
