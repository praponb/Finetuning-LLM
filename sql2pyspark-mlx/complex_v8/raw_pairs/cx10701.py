from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_plus_30m_to_date_by = Window.partitionBy(F.to_date("read_at_plus_30m")).orderBy(F.col("kwh").desc())
w_read_at_plus_30m_to_date_by_2 = Window.partitionBy(F.to_date("read_at_plus_30m")).orderBy(F.col("kwh").desc(), "id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_moving_6 = Window.orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_quality_read_at_plus_30m_to = Window.partitionBy("quality", F.to_date("read_at_plus_30m"))
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_ties = Window.orderBy("voltage")
w_read_at_plus_30m_to_date_by_3 = Window.partitionBy(F.to_date("read_at_plus_30m")).orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_plus_30m_to_date_by_4 = Window.partitionBy(F.to_date("read_at_plus_30m")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_windows = (
    filtered_readings
    .select(
        "*",
        F.round(F.cume_dist().over(w_read_at_plus_30m_to_date_by), 3).alias("kwh_cume_dist"),
        F.lag("days_since_read_at", 2).over(w_read_at_plus_30m_to_date_by_2).alias("prev2_days_since_read_at"),
        F.ntile(3).over(w_all_by_voltage).alias("voltage_tercile"),
        F.sum("kwh").over(w_all_by_voltage_moving_6).alias("kwh_sum_7"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.count(F.lit(1)).over(w_quality_read_at_plus_30m_to).alias("rows_in_quality_read_at_plus_30m_to_date"),
        F.nth_value("read_at_plus_30m", 2).over(w_all_by_voltage_full).alias("second_read_at_plus_30m"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_read_at_plus_30m_to), F.lit(0)), 4).alias("kwh_share_of_quality_read_at_plus_30m_to_date"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.round(F.avg("days_since_read_at").over(w_all_by_voltage_moving_6), 2).alias("days_since_read_at_avg_7"),
        F.max("voltage").over(w_read_at_plus_30m_to_date_by_3).alias("voltage_max_3"),
        F.sum("days_since_read_at").over(w_read_at_plus_30m_to_date_by_4).alias("running_days_since_read_at"),
    )
)

result = (
    with_windows
    .filter(F.col("voltage_max_3").between(341.2, 577.5))
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "read_at_plus_30m",
        "days_since_read_at",
        "prev2_days_since_read_at",
        "voltage_tercile",
        "voltage_dense_rank",
        "rows_in_quality_read_at_plus_30m_to_date",
        "second_read_at_plus_30m",
        "kwh_share_of_quality_read_at_plus_30m_to_date",
        "voltage_cume_dist",
        "days_since_read_at_avg_7",
        "running_days_since_read_at",
        F.concat(F.col("quality"), F.lit("#"), F.col("days_since_read_at").cast("string")).alias("quality_ref"),
        (F.floor(F.col("voltage_tercile") / 2) * 2).alias("voltage_tercile_bucket"),
    )
    .orderBy(F.col("days_since_read_at_avg_7").desc())
)
