from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
    )
    .orderBy(F.col("browser").desc())
)
