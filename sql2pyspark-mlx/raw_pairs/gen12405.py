from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("id", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("id", "customer_id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("browser", "customer_id")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.min("started_at").alias("started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.max("started_at").alias("last_started_at"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("span_minutes")
)
