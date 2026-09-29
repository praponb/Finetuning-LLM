from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("device", "id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.min("started_at").alias("first_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
)
