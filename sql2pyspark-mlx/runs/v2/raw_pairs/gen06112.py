from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "device",
        "country",
        "browser",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
    )
)
