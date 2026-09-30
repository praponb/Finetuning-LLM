from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "started_at",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
    )
    .orderBy("started_at")
)
