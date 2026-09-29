from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
    )
    .orderBy("country_filled")
)
