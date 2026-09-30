from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]))
    .select(
        "id",
        "browser",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
    .orderBy(F.col("browser_filled").desc())
)
