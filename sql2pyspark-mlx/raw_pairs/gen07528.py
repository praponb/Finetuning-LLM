from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "TH") & F.col("country").isin(["JP", "TH"]) & F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "id",
        "device",
        F.substring("browser", 1, 3).alias("browser_prefix"),
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)
