from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") != "Edge")
    .select(
        "device",
        "browser",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)
