from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select(
        "id",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.trim("device").alias("clean_device"),
    )
)
