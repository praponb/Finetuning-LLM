from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & (F.col("customer_id") == 8) & F.col("device").isin(["tablet", "mobile"]))
    .select(
        "id",
        "country",
        F.upper("country").alias("country_upper"),
        F.when(F.col("device").isin(["mobile", "desktop"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
    .orderBy("country_upper")
)
