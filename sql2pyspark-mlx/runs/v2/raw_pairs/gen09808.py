from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2023-07-29")
    .select(
        "id",
        "started_at",
        "browser",
        "customer_id",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.lower("country").alias("country_lower"),
    )
    .orderBy(F.col("id").desc(), "started_at")
    .limit(10)
)
