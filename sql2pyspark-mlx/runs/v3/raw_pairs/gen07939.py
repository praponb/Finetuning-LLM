from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "started_at",
        "device",
        "country",
        F.when(F.col("browser").isin(["Chrome", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
    )
)
