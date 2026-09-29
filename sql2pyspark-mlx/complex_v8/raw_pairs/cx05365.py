from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_by_event_count = Window.partitionBy("session_id").orderBy(F.col("event_count").desc(), "quality", "session_id")
w_session_id_by_event_count_ru = Window.partitionBy("session_id").orderBy(F.col("event_count").desc(), "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_by_session_voltag = Window.partitionBy("session_id").orderBy(F.col("session_voltage").desc(), "quality", "session_id")
w_session_id_event_count_by_ev = Window.partitionBy("session_id", "event_count").orderBy("event_count")
w_session_id_by_event_count_pr = Window.partitionBy("session_id").orderBy(F.col("event_count").desc(), "quality", "session_id").rowsBetween(-6, -1)

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    per_session
    .select(
        "*",
        F.round(F.cume_dist().over(w_session_id_by_event_count), 3).alias("event_count_cume_dist"),
        F.last("session_voltage", ignorenulls=True).over(w_session_id_by_event_count_ru).alias("last_known_session_voltage"),
        F.dense_rank().over(w_session_id_by_session_voltag).alias("session_voltage_dense_rank"),
        F.round(F.cume_dist().over(w_session_id_event_count_by_ev), 3).alias("event_count_cume_dist_2"),
        F.max("session_voltage").over(w_session_id_by_event_count_pr).alias("session_voltage_max_6_prior"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.avg("event_count_cume_dist").alias("overall_avg_event_count_cume_dist")))
    .filter(F.col("event_count_cume_dist") > F.col("overall_avg_event_count_cume_dist"))
    .drop("overall_avg_event_count_cume_dist")
)

result = (
    qualified
    .filter(F.col("last_known_session_voltage") <= 749.0)
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "session_voltage",
        "duration_min",
        "event_count_cume_dist_2",
        "session_voltage_max_6_prior",
        (F.col("duration_min") + F.col("session_voltage")).alias("duration_min_plus_session_voltage"),
    )
    .orderBy(
        F.col("duration_min_plus_session_voltage").desc_nulls_first(),
        F.col("quality").asc(),
    )
)
