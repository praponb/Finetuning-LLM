from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & (F.col("browser") == "Firefox"))
    .select(
        "country",
        "customer_id",
        "started_at",
        F.initcap(F.trim("country")).alias("country_title"),
        F.trim("country").alias("clean_country"),
    )
)
