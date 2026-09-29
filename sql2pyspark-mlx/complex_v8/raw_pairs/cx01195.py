from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_voltage = Window.partitionBy(F.year("read_at")).orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_quality_read_at_to_date_by_r = Window.partitionBy("quality", F.to_date("read_at")).orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_quality_by_voltage_m = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(-1, Window.currentRow)
w_quality_read_at_month = Window.partitionBy("quality", F.date_trunc("month", "read_at"))
w_all_by_read_at_prior_1 = Window.orderBy("read_at", "id").rowsBetween(-1, -1)
w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id")
w_quality_read_at_to_date_by_r_2 = Window.partitionBy("quality", F.to_date("read_at")).orderBy(F.col("read_at").desc_nulls_first(), "id")
w_site_id_quality_by_voltage_t = Window.partitionBy("site_id", "quality").orderBy("voltage")
w_all_by_last_known_read_at_pr = Window.orderBy("last_known_read_at", "id").rowsBetween(-3, -1)
w_site_id_read_at_tercile = Window.partitionBy("site_id", "read_at_tercile")
w_last_known_read_at_year_read = Window.partitionBy(F.year("last_known_read_at"), "read_at_tercile_2").orderBy("distinct_quality_in_site_id", "id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id")

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("voltage") < 240.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    recent_readings
    .select(
        "*",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_year_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.max("kwh").over(w_quality_read_at_to_date_by_r).alias("kwh_max_1_prior"),
        F.last("voltage", ignorenulls=True).over(w_site_id_quality_by_voltage_r).alias("last_known_voltage"),
        F.lead("kwh").over(w_all_by_read_at).alias("next_kwh"),
        F.last("read_at", ignorenulls=True).over(w_site_id_quality_by_voltage_r).alias("last_known_read_at"),
        F.sum("kwh").over(w_site_id_quality_by_voltage_m).alias("kwh_sum_2"),
        F.size(F.collect_set("site_id").over(w_quality_read_at_month)).alias("distinct_site_id_in_quality_read_at_month"),
        F.round(F.avg("voltage").over(w_all_by_read_at_prior_1), 2).alias("voltage_avg_1_prior"),
        F.max("kwh").over(w_site_id_quality).alias("site_id_quality_max_kwh"),
        F.ntile(3).over(w_quality_site_id_by_read_at).alias("read_at_tercile"),
        F.ntile(3).over(w_quality_read_at_to_date_by_r_2).alias("read_at_tercile_2"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.rank().over(w_site_id_quality_by_voltage_t).alias("voltage_rank"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_max_1_prior",
        "last_known_voltage",
        "next_kwh",
        "last_known_read_at",
        "distinct_site_id_in_quality_read_at_month",
        "voltage_avg_1_prior",
        "read_at_tercile_2",
        "kwh_share_of_site_id",
        "voltage_rank",
        F.sum("read_at_tercile").over(w_all_by_last_known_read_at_pr).alias("read_at_tercile_sum_3_prior"),
        F.round(F.col("read_at_tercile_2") - F.avg("read_at_tercile_2").over(w_site_id_read_at_tercile), 2).alias("read_at_tercile_2_vs_site_id_read_at_tercile_avg"),
        F.round(F.percent_rank().over(w_last_known_read_at_year_read), 3).alias("distinct_quality_in_site_id_percent_rank"),
        F.size(F.collect_set("site_id").over(w_distinct_quality_in_site_id)).alias("distinct_site_id_in_distinct_quality_in_site_id"),
        F.round(F.avg("voltage_rank").over(w_distinct_quality_in_site_id), 2).alias("distinct_quality_in_site_id_avg_voltage_rank"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "read_at",
        "kwh_max_1_prior",
        "next_kwh",
        "distinct_site_id_in_quality_read_at_month",
        "voltage_avg_1_prior",
        "read_at_tercile_2",
        "read_at_tercile_sum_3_prior",
        "distinct_quality_in_site_id_percent_rank",
        "distinct_site_id_in_distinct_quality_in_site_id",
        "distinct_quality_in_site_id_avg_voltage_rank",
    )
    .orderBy(
        F.col("distinct_quality_in_site_id_percent_rank").desc(),
        F.col("read_at_tercile_2").asc(),
    )
)
