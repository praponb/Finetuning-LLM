from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-08-05")
    .select(
        "id",
        "browser",
        "country",
        "customer_id",
        F.initcap(F.trim("country")).alias("country_title"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(5)
)
