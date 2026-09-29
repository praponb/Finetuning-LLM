from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level").orderBy(F.col("log_ts").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

ranked = (
    flattened_attrs
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "log_ts", "k", "status_code_high", "is_flagged")
)

with_prev = ranked.withColumn("prev_log_ts", F.lag("log_ts").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

result = (
    sessions_app_logs
    .select("id", "k", "status_code_high", "prev_log_ts", "is_new_session")
    .orderBy(F.col("is_new_session").asc_nulls_first())
)
