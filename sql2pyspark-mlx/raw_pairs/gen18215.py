from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "customer_id",
        "country",
        F.lower("browser").alias("browser_lower"),
    )
)
