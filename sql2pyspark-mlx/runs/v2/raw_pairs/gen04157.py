from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "customer_id",
        "device",
        "started_at",
        F.concat("browser", F.lit(" - "), "country").alias("browser_country"),
    )
)
