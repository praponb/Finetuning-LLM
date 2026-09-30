from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-11-15") | (F.col("browser") == "Safari"))
    .select(
        "id",
        "country",
        "device",
        F.initcap(F.trim("country")).alias("country_title"),
        F.when(F.col("device").isin(["mobile", "desktop"]), "Core").otherwise("Other").alias("device_group"),
    )
)
