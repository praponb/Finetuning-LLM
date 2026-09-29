from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & (F.col("device") == "desktop"))
    .select(
        "id",
        "country",
        "device",
        "customer_id",
        F.trim("device").alias("clean_device"),
        F.trim("country").alias("clean_country"),
    )
)
