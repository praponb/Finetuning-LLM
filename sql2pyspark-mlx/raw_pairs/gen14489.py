from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "k").orderBy(F.col("log_ts").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flagged = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

with_gaps = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "session_id",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "session_id",
        "row_num",
        F.lead("session_id").over(w).alias("next_session_id"),
        F.lead("session_id_2").over(w).alias("next_session_id_2"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "session_id",
        "next_session_id_2",
        "seq_no",
    )
    .orderBy(F.col("next_session_id_2").desc_nulls_first())
)
