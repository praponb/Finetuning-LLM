from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "host").orderBy(F.col("log_ts_day").desc(), "id", "k")

flattened = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("k", "value"))
)

base = (
    flattened
    .filter(F.col("host") == "h2")
    .withColumn("k_group", F.when(F.col("k").isin(["trace", "version"]), "Group A").otherwise("Other"))
    .withColumn("host_group", F.when(F.col("host").isin(["h4", "h1"]), "Priority").otherwise("Other"))
)

base_app_logs = (
    base
    .filter((F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp")) & (F.col("host") != "h1"))
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "k_group",
        "host_group",
        F.substring("host", 1, 3).alias("host_prefix"),
        F.col("log_ts").cast("date").alias("log_ts_day"),
    )
)

enriched = (
    base_app_logs
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
)

base_app_logs_2 = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (~F.col("k").eqNullSafe(F.lit("version"))))
    .select("*", F.concat_ws("/", "host", "k").alias("host_k_key"))
)

result = (
    base_app_logs_2
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("event_seq_per_h3"),
        F.avg(F.when(F.col("host") == "h4", F.col("event_seq_2"))).alias("h4_avg_event_seq_2"),
        F.sum(F.when(F.col("k") == "trace", F.col("event_seq")).otherwise(0)).alias("trace_event_seq"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
        F.count_if(F.col("k") == "trace").alias("trace_events"),
    )
)
