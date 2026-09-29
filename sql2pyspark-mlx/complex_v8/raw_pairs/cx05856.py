from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_prior_1 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_site_tariff_site_id_by_read = Window.partitionBy("site_tariff", "site_id").orderBy("read_at", "id").rowsBetween(-3, -1)
w_read_at_month_quality = Window.partitionBy(F.date_trunc("month", "read_at"), "quality")
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").asc_nulls_last())
w_site_id_quality_by_read_at_r = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_tariff_by_kwh_minus_vol = Window.partitionBy("site_tariff").orderBy(F.col("kwh_minus_voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_kwh_minus_vol_2 = Window.partitionBy("site_tariff").orderBy(F.col("kwh_minus_voltage").asc_nulls_last(), "id")
w_quality_site_id_by_read_at_f = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_read_at = Window.partitionBy("site_tariff").orderBy("read_at", "id")
w_site_tariff_by_read_at_ties = Window.partitionBy("site_tariff").orderBy("read_at")
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last())
w_site_tariff_site_id_by_read_2 = Window.partitionBy("site_tariff", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_by_read_at_runni = Window.partitionBy("site_tariff").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 223.0) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

combined_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isin("EV", "TOU"))
    .select("s.*", F.col("si.tariff").alias("site_tariff"), "si.site_name")
)

with_windows = (
    combined_details
    .select(
        "id",
        "quality",
        "kwh_minus_voltage",
        "site_tariff",
        "site_name",
        F.sum("kwh_minus_voltage").over(w_all_by_read_at_prior_1).alias("kwh_minus_voltage_sum_1_prior"),
        F.round(F.avg("voltage").over(w_site_tariff_site_id_by_read), 2).alias("voltage_avg_3_prior"),
        F.min("voltage").over(w_read_at_month_quality).alias("read_at_month_quality_min_voltage"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.last("kwh_minus_voltage", ignorenulls=True).over(w_site_id_quality_by_read_at_r).alias("last_known_kwh_minus_voltage"),
        F.size(F.collect_set("site_tariff").over(w_site_id_quality)).alias("distinct_site_tariff_in_site_id_quality"),
        F.size(F.collect_set("site_tariff").over(w_read_at_month_quality)).alias("distinct_site_tariff_in_read_at_month_quality"),
        F.nth_value("kwh_minus_voltage", 2).over(w_site_tariff_by_kwh_minus_vol).alias("second_kwh_minus_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_site_tariff_by_kwh_minus_vol_2)).alias("voltage_change"),
        F.last("kwh_minus_voltage").over(w_quality_site_id_by_read_at_f).alias("last_kwh_minus_voltage"),
        F.ntile(10).over(w_site_tariff_by_read_at).alias("read_at_decile"),
        F.round(F.col("kwh_minus_voltage") - F.avg("kwh_minus_voltage").over(w_read_at_month_quality), 2).alias("kwh_minus_voltage_vs_read_at_month_quality_avg"),
        F.round(F.percent_rank().over(w_site_tariff_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.dense_rank().over(w_site_id_quality_by_read_at_t).alias("read_at_dense_rank_2"),
        F.sum("voltage").over(w_site_tariff_site_id_by_read_2).alias("running_voltage"),
        F.min("kwh_minus_voltage").over(w_site_tariff_by_read_at_runni).alias("min_kwh_minus_voltage_so_far"),
    )
)

result = (
    with_windows
    .select("read_at_percent_rank")
    .distinct()
    .orderBy(F.col("read_at_percent_rank").desc())
)
