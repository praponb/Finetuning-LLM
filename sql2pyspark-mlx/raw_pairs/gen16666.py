from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.orderBy("log_ts", "id", "key")
w3 = Window.partitionBy(F.to_date("log_ts"), "key").orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
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

flattened_attrs = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

enriched = (
    flattened_attrs
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("next_status_code_high", F.lead("status_code_high", 1).over(w3))
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w4))

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w5).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w5) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w6))

remaining_keys = (
    islands
    .select("service", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h1")
        .select("service", "id")
    )
)

verified = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("burst_id")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.countDistinct("key").alias("distinct_keys"),
    )
    .orderBy(F.col("distinct_keys").asc_nulls_last())
)
