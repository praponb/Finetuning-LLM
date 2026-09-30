from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_burst_id_ties = Window.orderBy(F.col("burst_id").desc_nulls_first())
w_all_by_burst_id = Window.orderBy(F.col("burst_id").desc_nulls_first(), "id")
w_is_new_session = Window.partitionBy("is_new_session")
w_is_new_session_device_by_pre = Window.partitionBy("is_new_session", "device").orderBy("prev_played_at", "id")

base_plays = df_plays

with_prev = base_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_device_running))

screened = (
    sessionized
    .crossJoin(sessionized.agg(F.avg("burst_id").alias("overall_avg_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_avg_burst_id"))
    .drop("overall_avg_burst_id")
)

with_windows = (
    screened
    .select(
        "*",
        F.rank().over(w_all_by_burst_id_ties).alias("burst_id_rank"),
        F.row_number().over(w_all_by_burst_id).alias("burst_id_seq"),
        F.round(F.col("burst_id") - F.avg("burst_id").over(w_is_new_session), 2).alias("burst_id_vs_is_new_session_avg"),
        F.dense_rank().over(w_is_new_session_device_by_pre).alias("prev_played_at_dense_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "burst_id",
        "burst_id_rank",
        "burst_id_seq",
        "burst_id_vs_is_new_session_avg",
        F.date_format(F.col("prev_played_at"), "MMM yyyy").alias("prev_played_at_label"),
    )
    .orderBy("prev_played_at_label", "id")
    .limit(20)
)
