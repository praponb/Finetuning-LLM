from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_read_at_to_date_by_read_at = Window.partitionBy(F.to_date("read_at")).orderBy("read_at", "id")
w_read_at_to_date_by_read_at_r = Window.partitionBy(F.to_date("read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_by_read_at_f = Window.partitionBy(F.to_date("read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_kwh = Window.partitionBy("quality", "site_zone").orderBy("kwh", "id")
w_site_zone_site_id_by_kwh_tie = Window.partitionBy("site_zone", "site_id").orderBy(F.col("kwh").desc_nulls_first())
w_quality_site_zone_by_kwh_run = Window.partitionBy("quality", "site_zone").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_zone_by_kwh_ful = Window.partitionBy("quality", "site_zone").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_kwh_tie = Window.partitionBy("quality", "site_zone").orderBy("kwh")
w_all_by_kwh_percent_rank = Window.orderBy(F.col("kwh_percent_rank").asc_nulls_last(), "id")
w_read_at_year_by_kwh_seq_prio = Window.partitionBy(F.year("read_at")).orderBy(F.col("kwh_seq").desc(), "id").rowsBetween(-1, -1)
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_all_by_max_voltage_so_far = Window.orderBy("max_voltage_so_far", "id")

scoped_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 236.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_details_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        F.col("si.zone").alias("site_zone"),
    )
)

with_trends = (
    with_details_site
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "site_zone",
        F.sum("kwh").over(w_quality_site_zone).alias("quality_site_zone_sum_kwh"),
        F.round(F.cume_dist().over(w_read_at_to_date_by_read_at), 3).alias("read_at_cume_dist"),
        F.max("voltage").over(w_read_at_to_date_by_read_at_r).alias("max_voltage_so_far"),
        F.nth_value("voltage", 2).over(w_read_at_to_date_by_read_at_f).alias("second_voltage"),
        F.round(F.percent_rank().over(w_read_at_to_date_by_read_at), 3).alias("read_at_percent_rank"),
        F.count(F.lit(1)).over(w_read_at_to_date_by_read_at_r).alias("running_count"),
        F.ntile(10).over(w_quality_site_zone_by_kwh).alias("kwh_decile"),
        F.round(F.avg("voltage").over(w_quality_site_zone), 2).alias("quality_site_zone_avg_voltage"),
        F.dense_rank().over(w_site_zone_site_id_by_kwh_tie).alias("kwh_dense_rank"),
        F.rank().over(w_site_zone_site_id_by_kwh_tie).alias("kwh_rank"),
        F.row_number().over(w_quality_site_zone_by_kwh).alias("kwh_seq"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_zone_by_kwh_run).alias("last_known_read_at"),
        F.nth_value("voltage", 2).over(w_quality_site_zone_by_kwh_ful).alias("second_voltage_2"),
        F.round(F.percent_rank().over(w_quality_site_zone_by_kwh_tie), 3).alias("kwh_percent_rank"),
    )
)

leaders = (
    with_trends
    .filter((F.col("kwh_dense_rank") == 1) & (F.col("site_zone").isNotNull()))
    .drop("kwh_dense_rank")
)

ranked = (
    leaders
    .select(
        "*",
        F.lead("max_voltage_so_far", 1, 0).over(w_all_by_kwh_percent_rank).alias("next_max_voltage_so_far"),
        F.round(F.avg("kwh_decile").over(w_read_at_year_by_kwh_seq_prio), 2).alias("kwh_decile_avg_1_prior"),
        F.count(F.lit(1)).over(w_read_at_to_date).alias("rows_in_read_at_to_date"),
        F.size(F.collect_set("site_zone").over(w_read_at_to_date)).alias("distinct_site_zone_in_read_at_to_date"),
        F.round((F.unix_timestamp("last_known_read_at") - F.unix_timestamp(F.lag("last_known_read_at").over(w_all_by_max_voltage_so_far))) / 60, 1).alias("minutes_since_prev_last_known_read_at"),
        F.ntile(3).over(w_all_by_max_voltage_so_far).alias("max_voltage_so_far_tercile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "quality",
        "quality_slug",
        "quality_site_zone_sum_kwh",
        "read_at_cume_dist",
        "second_voltage",
        "read_at_percent_rank",
        "running_count",
        "kwh_decile",
        "kwh_rank",
        "kwh_seq",
        "last_known_read_at",
        "next_max_voltage_so_far",
        "kwh_decile_avg_1_prior",
        "rows_in_read_at_to_date",
        "minutes_since_prev_last_known_read_at",
        "max_voltage_so_far_tercile",
        F.concat(F.col("quality_slug"), F.lit("#"), F.col("kwh_rank").cast("string")).alias("quality_slug_ref"),
        F.lower(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
)
