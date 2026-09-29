from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & F.col("device").isin(["desktop", "mobile", "tablet"]) & F.col("country").isin(["JP", "SG"]))
    .select(
        "id",
        "started_at",
        "device",
        "browser",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.month("started_at").alias("started_at_month"),
    )
)
