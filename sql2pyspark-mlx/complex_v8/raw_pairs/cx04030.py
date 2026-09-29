from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc_nulls_first(), "id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").asc_nulls_last())
w_quality_read_at_year_by_read = Window.partitionBy("quality", F.year("read_at")).orderBy(F.col("read_at").desc(), "id")
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc_nulls_first())
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_quality_read_at_year_by_read_2 = Window.partitionBy("quality", F.year("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_prev_read_at_year_month_by_k = Window.partitionBy(F.date_trunc("month", "prev_read_at_year")).orderBy(F.col("kwh_vs_read_at_year_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_year_next2_read = Window.partitionBy(F.year("read_at_year"), F.date_trunc("month", "next2_read_at"))

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

with_windows = (
    valid_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_year",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.lag("read_at_year").over(w_all_by_voltage).alias("prev_read_at_year"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year), 2).alias("kwh_vs_read_at_year_avg"),
        F.dense_rank().over(w_quality_site_id_by_voltage_t).alias("voltage_dense_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_read_at_year_by_read))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_year), F.lit(0)), 4).alias("kwh_share_of_read_at_year"),
        F.round(F.percent_rank().over(w_all_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.lag("read_at", 3).over(w_quality_read_at_year_by_read).alias("prev3_read_at"),
        F.sum("voltage").over(w_quality_site_id_by_voltage_p).alias("voltage_sum_2_prior"),
        F.max("voltage").over(w_quality_read_at_year_by_read_2).alias("voltage_max_2"),
        F.max("voltage").over(w_read_at_year).alias("read_at_year_max_voltage"),
        F.lead("read_at", 2).over(w_quality_site_id_by_voltage).alias("next2_read_at"),
    )
)

leaders = (
    with_windows
    .filter(F.col("voltage_dense_rank") <= 3)
    .drop("voltage_dense_rank")
)

with_trends = (
    leaders
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_vs_site_id_quality_avg",
        "prev_read_at_year",
        "kwh_vs_read_at_year_avg",
        "kwh_share_of_read_at_year",
        "voltage_percent_rank",
        "prev3_read_at",
        "read_at_year_max_voltage",
        F.last("kwh_vs_read_at_year_avg", ignorenulls=True).over(w_prev_read_at_year_month_by_k).alias("last_known_kwh_vs_read_at_year_avg"),
        F.round(F.col("read_at_year_max_voltage") / F.nullif(F.sum("read_at_year_max_voltage").over(w_read_at_year_year_next2_read), F.lit(0)), 4).alias("read_at_year_max_voltage_share_of_read_at_year_y"),
        F.count(F.lit(1)).over(w_read_at_year_year_next2_read).alias("rows_in_read_at_year_year_next2_read_at_month"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "prev_read_at_year",
        "kwh_vs_read_at_year_avg",
        "kwh_share_of_read_at_year",
        "read_at_year_max_voltage",
        "last_known_kwh_vs_read_at_year_avg",
        "rows_in_read_at_year_year_next2_read_at_month",
    )
    .orderBy(
        F.col("rows_in_read_at_year_year_next2_read_at_month").desc_nulls_first(),
    )
)
