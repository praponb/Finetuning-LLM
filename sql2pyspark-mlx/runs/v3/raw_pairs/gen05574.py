from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 7, 35]))
    .select(
        "country",
        F.upper("browser").alias("browser_upper"),
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)
