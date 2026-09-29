from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_hour_month_by_voltag = Window.partitionBy(F.date_trunc("month", "read_at_hour")).orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_hour_month_by_voltag_2 = Window.partitionBy(F.date_trunc("month", "read_at_hour")).orderBy("voltage", "id")
w_all_by_least_kwh_voltage = Window.orderBy(F.col("least_kwh_voltage").desc(), "id")
w_quality_by_read_at_hour = Window.partitionBy("quality").orderBy(F.col("read_at_hour").desc_nulls_first(), "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_hour = Window.orderBy(F.col("read_at_hour").desc(), "id")
w_read_at_hour_year_quality = Window.partitionBy(F.year("read_at_hour"), "quality")
w_all_by_least_kwh_voltage_tie = Window.orderBy(F.col("least_kwh_voltage").desc())
w_read_at_hour_month_by_voltag_3 = Window.partitionBy(F.date_trunc("month", "read_at_hour")).orderBy("voltage")

recent_readings = (
    df_meter_readings
    .filter((F.col("voltage") >= 218.2) & (F.col("read_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

with_trends = (
    recent_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "least_kwh_voltage",
        F.last("kwh").over(w_read_at_hour_month_by_voltag).alias("last_kwh"),
        F.round((F.unix_timestamp("read_at_hour") - F.unix_timestamp(F.lag("read_at_hour").over(w_read_at_hour_month_by_voltag_2))) / 60, 1).alias("minutes_since_prev_read_at_hour"),
        F.ntile(10).over(w_all_by_least_kwh_voltage).alias("least_kwh_voltage_decile"),
        F.dense_rank().over(w_read_at_hour_month_by_voltag_2).alias("voltage_dense_rank"),
        F.lag("read_at_hour", 2).over(w_quality_by_read_at_hour).alias("prev2_read_at_hour"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.ntile(10).over(w_all_by_read_at_hour).alias("read_at_hour_decile"),
        F.count(F.lit(1)).over(w_read_at_hour_year_quality).alias("rows_in_read_at_hour_year_quality"),
        F.dense_rank().over(w_all_by_least_kwh_voltage_tie).alias("least_kwh_voltage_dense_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_hour_year_quality), F.lit(0)), 4).alias("kwh_share_of_read_at_hour_year_quality"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.rank().over(w_read_at_hour_month_by_voltag_3).alias("voltage_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "least_kwh_voltage",
        "last_kwh",
        "least_kwh_voltage_decile",
        "voltage_dense_rank",
        "prev2_read_at_hour",
        "rows_in_site_id",
        "read_at_hour_decile",
        "rows_in_read_at_hour_year_quality",
        "least_kwh_voltage_dense_rank",
        "voltage_share_of_site_id",
        "voltage_rank",
    )
    .orderBy(
        F.col("read_at_hour_decile").asc_nulls_last(),
        F.col("kwh").asc_nulls_last(),
    )
)
