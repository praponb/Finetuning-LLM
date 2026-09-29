from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "browser",
        "started_at",
        "customer_id",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)
