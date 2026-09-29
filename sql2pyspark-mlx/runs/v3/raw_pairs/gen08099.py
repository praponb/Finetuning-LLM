from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & ((F.col("browser") == "Chrome") | (F.col("country") != "SG")))
    .select(
        "started_at",
        F.year("started_at").alias("started_at_year"),
        F.length("country").alias("country_length"),
    )
    .orderBy("started_at_year")
)
