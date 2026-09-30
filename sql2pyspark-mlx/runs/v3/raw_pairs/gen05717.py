from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 25) & (F.col("browser") != "Safari"))
    .select(
        "browser",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
    )
)
