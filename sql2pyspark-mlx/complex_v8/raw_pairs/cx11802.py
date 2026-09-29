from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_read_at_to_date_by_kwh = Window.partitionBy(F.to_date("prev_read_at")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_read_at_month_quality = Window.partitionBy(F.month("prev_read_at"), "quality")
w_new_session_flag_site_id_by = Window.partitionBy("new_session_flag", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_read_at_r = Window.partitionBy("site_id", "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_read_at_to_date_by_kwh_2 = Window.partitionBy(F.to_date("prev_read_at")).orderBy(F.col("kwh").desc())
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

with_trends = (
    sessionized
    .select(
        "*",
        F.first("kwh").over(w_prev_read_at_to_date_by_kwh).alias("first_kwh"),
        F.min("session_id").over(w_prev_read_at_month_quality).alias("prev_read_at_month_quality_min_session_id"),
        F.nth_value("session_id", 2).over(w_new_session_flag_site_id_by).alias("second_session_id"),
        F.last("prev_read_at", ignorenulls=True).over(w_site_id_quality_by_read_at_r).alias("last_known_prev_read_at"),
        F.round(F.cume_dist().over(w_prev_read_at_to_date_by_kwh_2), 3).alias("kwh_cume_dist"),
        F.lag("voltage_minus_kwh", 1, 0).over(w_site_id_quality_by_read_at).alias("prev_voltage_minus_kwh"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "prev_read_at",
        "session_id",
        "first_kwh",
        "prev_read_at_month_quality_min_session_id",
        "second_session_id",
        "last_known_prev_read_at",
        "prev_voltage_minus_kwh",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("prev_read_at"))) / 3600, 1).alias("hours_prev_read_at_to_read_at"),
    )
    .orderBy(F.col("prev_read_at").desc())
)
