from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "browser",
        "started_at",
        F.substring("browser", 1, 4).alias("browser_prefix"),
        F.when(F.col("browser").isin(["Edge", "Firefox"]), "Priority").otherwise("Other").alias("browser_group"),
    )
    .orderBy("started_at")
)
