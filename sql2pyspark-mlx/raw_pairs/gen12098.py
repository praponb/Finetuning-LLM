from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "host").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.partitionBy("service", "key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("log_ts"), "service").orderBy(F.col("log_ts").desc(), "id", "key")
w4 = Window.partitionBy("level", "service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w6 = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w7 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("key").orderBy(F.col("session_no").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

ranked = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "service", "log_ts", "level", "key")
)

scored = (
    ranked
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "key",
        F.first("key", ignorenulls=True).over(w2).alias("first_known_key"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("key", ignorenulls=True).over(w4).alias("first_known_key_2"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

flagged = (
    scored
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w6).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w6) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "service",
        "log_ts",
        "key",
        "first_known_key",
        "first_known_key_2",
        F.sum("is_new_session").over(w7).alias("session_no"),
    )
)

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "log_ts", "key", "first_known_key_2", "rn")
    .orderBy(F.col("rn").asc_nulls_first(), "id")
)
