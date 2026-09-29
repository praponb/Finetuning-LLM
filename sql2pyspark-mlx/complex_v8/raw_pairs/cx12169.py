from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_read_at_full = Window.orderBy(F.col("prev_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id_starts_session = Window.partitionBy("site_id", "starts_session")
w_all_by_prev_read_at_ties = Window.orderBy(F.col("prev_read_at").desc())
w_all_by_prev_read_at = Window.orderBy(F.col("prev_read_at").desc(), "id")

prepared = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

with_trends = (
    sessionized
    .select(
        "*",
        F.nth_value("starts_session", 2).over(w_all_by_prev_read_at_full).alias("second_starts_session"),
        F.row_number().over(w_all_by_read_at).alias("seq_no"),
        F.round(F.col("starts_session") / F.nullif(F.sum("starts_session").over(w_site_id_starts_session), F.lit(0)), 4).alias("starts_session_share_of_site_id_starts_session"),
        F.dense_rank().over(w_all_by_prev_read_at_ties).alias("prev_read_at_dense_rank"),
        F.sum("burst_id").over(w_site_id_starts_session).alias("site_id_starts_session_sum_burst_id"),
        F.round((F.unix_timestamp("prev_read_at") - F.unix_timestamp(F.lag("prev_read_at").over(w_all_by_prev_read_at))) / 60, 1).alias("minutes_since_prev_prev_read_at"),
    )
)

result = (
    with_trends
    .groupBy("prev_read_at_dense_rank", "prev_read_at")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.count("second_starts_session").alias("count_v"), F.max("second_starts_session").alias("max_v"))
)
