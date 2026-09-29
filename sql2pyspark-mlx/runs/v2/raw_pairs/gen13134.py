from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome", "Safari"]))
    .select(
        "id",
        "started_at",
        "customer_id",
        "browser",
        F.when(F.col("country").isin(["TH", "UK"]), "Core").otherwise("Other").alias("country_group"),
    )
)
