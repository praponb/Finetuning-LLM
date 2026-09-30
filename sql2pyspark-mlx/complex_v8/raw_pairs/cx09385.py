from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_new_session_by_kwh = Window.partitionBy("is_new_session").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_is_new_session_by_kwh_moving = Window.partitionBy("is_new_session").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy("read_at")
w_is_new_session_by_kwh_ties = Window.partitionBy("is_new_session").orderBy(F.col("kwh").desc_nulls_first())
w_is_new_session = Window.partitionBy("is_new_session")

base_readings = (
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

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_site_id_running))

with_trends = (
    sessionized
    .select(
        "*",
        (F.col("voltage") - F.lag("voltage").over(w_is_new_session_by_kwh)).alias("voltage_change"),
        F.max("burst_id").over(w_is_new_session_by_kwh_moving).alias("burst_id_max_4"),
        F.round(F.percent_rank().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_percent_rank"),
        F.dense_rank().over(w_is_new_session_by_kwh_ties).alias("kwh_dense_rank"),
        F.min("burst_id").over(w_is_new_session).alias("is_new_session_min_burst_id"),
    )
)

result = (
    with_trends
    .filter(F.month(F.col("read_at")).isin(1, 3))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "is_new_session",
        "voltage_change",
        "read_at_percent_rank",
        "kwh_dense_rank",
        F.concat(F.col("quality"), F.lit("#"), F.col("burst_id_max_4").cast("string")).alias("quality_ref"),
        F.coalesce(F.col("read_at_percent_rank"), F.lit(0.0)).alias("read_at_percent_rank_filled"),
    )
    .orderBy(
        F.col("voltage_change").desc_nulls_first(),
        F.col("kwh").asc_nulls_last(),
    )
)
