from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 13) & (F.col("device").isin(["mobile", "tablet", "desktop"]) | F.col("browser").isin(["Firefox", "Chrome"])))
    .select(
        "browser",
        "started_at",
        "device",
        F.when(F.col("device").isin(["tablet", "mobile"]), "Core").otherwise("Other").alias("device_group"),
    )
    .orderBy("browser")
)
