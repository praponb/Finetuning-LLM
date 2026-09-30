from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service", "key").orderBy(F.col("last3_max_status_code").desc_nulls_first(), "id", "key")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w6 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

enriched = (
    flattened
    .withColumn("last3_count_status_code", F.count("status_code").over(w))
    .withColumn("last3_max_status_code", F.max("status_code").over(w))
    .withColumn("last3_min_status_code", F.min("status_code").over(w))
)

gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    gaps
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        "value",
        "last3_max_status_code",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

deduped = (
    sessions_app_logs
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

gaps_2 = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w5).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w5) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps_2
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        F.sum("session_start_flag").over(w6).alias("session_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.min("session_id").alias("min_session_id"),
    )
)
