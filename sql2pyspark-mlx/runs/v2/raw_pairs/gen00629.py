from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.lower("browser").alias("browser_lower"),
    )
)
