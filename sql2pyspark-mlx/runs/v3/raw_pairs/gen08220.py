from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("device") == "tablet") | F.col("customer_id").isin([14, 30, 36])))
    .select(
        "id",
        "country",
        "device",
        "started_at",
        F.upper("country").alias("country_upper"),
    )
    .orderBy(F.col("id").desc())
)
