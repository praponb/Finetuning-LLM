from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "browser",
        "country",
        "device",
        F.length("country").alias("country_length"),
    )
    .orderBy("browser")
)
