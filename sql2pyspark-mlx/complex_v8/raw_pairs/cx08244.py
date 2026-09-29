from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_moving_3 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_site_id_quality_by_voltage_f = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_month = Window.partitionBy("site_id", F.month("read_at"))

valid = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "read_at",
        "read_at_label",
        F.round(F.avg("voltage").over(w_all_by_read_at_moving_3), 2).alias("voltage_avg_4"),
        F.ntile(10).over(w_all_by_read_at).alias("read_at_decile"),
        F.nth_value("read_at", 2).over(w_site_id_quality_by_voltage_f).alias("second_read_at"),
        F.min("kwh").over(w_site_id_read_at_month).alias("site_id_read_at_month_min_kwh"),
    )
)

result = (
    windowed
    .filter(F.col("site_id_read_at_month_min_kwh") < 960.0)
    .select(
        "id",
        "read_at",
        "read_at_decile",
        "second_read_at",
        "site_id_read_at_month_min_kwh",
        (F.floor(F.col("voltage_avg_4") / 5) * 5).alias("voltage_avg_4_bucket"),
    )
    .orderBy(F.col("read_at").desc(), F.col("site_id_read_at_month_min_kwh").desc())
)
