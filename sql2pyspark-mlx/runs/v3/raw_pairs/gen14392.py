from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.when(F.col("browser").isin(["Chrome", "Safari"]), "Priority").otherwise("Other").alias("browser_group"),
    )
)
