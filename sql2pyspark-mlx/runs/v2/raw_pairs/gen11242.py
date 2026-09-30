from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "started_at",
        "customer_id",
        "browser",
        F.when(F.col("country").isin(["JP", "SG"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)
