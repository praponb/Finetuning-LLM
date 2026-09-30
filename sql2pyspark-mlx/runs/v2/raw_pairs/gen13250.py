from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "country",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
        F.when(F.col("browser").isin(["Edge", "Chrome"]), "Priority").otherwise("Other").alias("browser_group"),
    )
)
