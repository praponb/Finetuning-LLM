from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
    )
)
