from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_zone_site_id = Window.partitionBy("site_zone", "site_id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_voltage = Window.partitionBy("quality", "site_zone").orderBy("voltage", "id").rowsBetween(-6, -1)
w_site_id_site_zone_by_read_at = Window.partitionBy("site_id", "site_zone").orderBy(F.col("read_at").desc())
w_all_by_kwh_moving_1 = Window.orderBy("kwh", "id").rowsBetween(-1, Window.currentRow)
w_site_id_site_zone_by_read_at_2 = Window.partitionBy("site_id", "site_zone").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_read_at_month_by_v = Window.partitionBy("site_zone", F.date_trunc("month", "read_at")).orderBy(F.col("voltage_per_kwh").desc_nulls_first(), "id").rowsBetween(-2, -1)

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 78.6) & (F.col("site_id") <= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

combined_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced = (
    combined_details_site
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.round(F.col("voltage_per_kwh") - F.avg("voltage_per_kwh").over(w_site_zone_site_id), 2).alias("voltage_per_kwh_vs_site_zone_site_id_avg"),
        F.rank().over(w_site_id_by_read_at_ties).alias("read_at_rank"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_kwh)).alias("voltage_change"),
        F.first("site_installed_kw").over(w_all_by_voltage_full).alias("first_site_installed_kw"),
        F.sum("voltage").over(w_quality_site_zone_by_voltage).alias("voltage_sum_6_prior"),
        F.round(F.percent_rank().over(w_site_id_site_zone_by_read_at), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.max("site_installed_kw").over(w_all_by_kwh_moving_1).alias("site_installed_kw_max_2"),
        F.nth_value("voltage", 2).over(w_site_id_site_zone_by_read_at_2).alias("second_voltage"),
        F.max("voltage").over(w_site_zone_read_at_month_by_v).alias("voltage_max_2_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "voltage_per_kwh",
        "site_installed_kw",
        "voltage_vs_quality_avg",
        "voltage_per_kwh_vs_site_zone_site_id_avg",
        "read_at_rank",
        "distinct_site_id_in_quality",
        "voltage_change",
        "voltage_sum_6_prior",
        "read_at_percent_rank",
        "site_installed_kw_max_2",
        "second_voltage",
    )
    .orderBy("id", F.col("site_id").asc_nulls_last())
)
