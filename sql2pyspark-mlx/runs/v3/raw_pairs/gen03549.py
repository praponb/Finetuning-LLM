from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge"]))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.trim("device").alias("clean_device"),
    )
)
