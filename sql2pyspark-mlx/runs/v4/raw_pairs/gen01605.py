from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host", "key")
w7 = Window.partitionBy("level").orderBy("seq_no", "id", "key")
w8 = Window.orderBy(F.col("seq_no").desc(), "id", "key")
w9 = Window.partitionBy("service").orderBy("rn", "id", "key")
w10 = Window.partitionBy("key").orderBy("event_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("level", "host").orderBy("prev_seq_no", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("service").orderBy("event_seq", "id", "key")
w13 = Window.partitionBy("level").orderBy("seq_no_share", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

with_windows = (
    exploded
    .select(
        "*",
        F.first("key", ignorenulls=True).over(w).alias("first_known_key"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w4) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w5))

with_windows_app_logs = (
    sessions_app_logs
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4))
    .withColumn("prev_seq_no", F.lag("seq_no", 2).over(w7))
    .withColumn("event_seq", F.row_number().over(w8))
)

enriched = (
    with_windows_app_logs
    .select(
        "*",
        F.lag("event_seq", 1).over(w9).alias("prev_event_seq"),
        F.sum("burst_id").over(w10).alias("running_burst_id"),
        F.sum("event_seq").over(w11).alias("cumulative_event_seq"),
        F.lead("event_seq", 1, 0).over(w12).alias("next_event_seq"),
        F.last("is_new_burst", ignorenulls=True).over(w13).alias("last_known_is_new_burst"),
    )
)

result = (
    enriched
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "key",
        "attr_value",
        "first_known_key",
        "seq_no",
        "rn",
        F.when(F.col("service").rlike("^(checkout|auth)$"), "EXTENDED").when(F.col("service").rlike("^(billing|search)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy(F.col("level").asc_nulls_last())
)
