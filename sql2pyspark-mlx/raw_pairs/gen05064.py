from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        "customer_id",
        "device",
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)
