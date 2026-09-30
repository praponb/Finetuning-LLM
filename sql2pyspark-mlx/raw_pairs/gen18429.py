from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("started_at") >= "2023-04-12")
        .select("customer_id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("edge_rows").desc())
)
