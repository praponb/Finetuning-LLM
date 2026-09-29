from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_prior_2 = Window.orderBy("voltage", "id").rowsBetween(-2, -1)
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_ties = Window.orderBy("voltage")
w_site_id_read_at_year_by_kwh = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_voltage_max_2_prior_tier_sit = Window.partitionBy("voltage_max_2_prior_tier", "site_id")
w_all_by_running_kwh_prior_2 = Window.orderBy("running_kwh", "id").rowsBetween(-2, -1)
w_all_by_running_kwh_running = Window.orderBy("running_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_kwh_full = Window.orderBy("running_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_meter_readings
    .filter(F.col("site_id") <= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

windowed = (
    scoped
    .select(
        "id",
        "site_id",
        "kwh",
        F.max("voltage").over(w_all_by_voltage_prior_2).alias("voltage_max_2_prior"),
        F.nth_value("voltage", 2).over(w_all_by_voltage_full).alias("second_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.sum("kwh").over(w_site_id_read_at_year_by_kwh).alias("running_kwh"),
    )
)

scored = (
    windowed
    .filter(F.col("kwh") <= 113.1)
    .select(
        "*",
        F.greatest(F.col("running_kwh"), F.col("voltage_max_2_prior")).alias("greatest_running_kwh_voltage_max_2_prior"),
        F.coalesce(F.col("running_kwh"), F.lit(0.0)).alias("running_kwh_filled"),
        F.abs(F.col("second_voltage") - 227.1).alias("second_voltage_distance"),
    )
)

with_parent = (
    scored.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage_max_2_prior",
        "s.second_voltage",
        "s.running_kwh",
        "s.greatest_running_kwh_voltage_max_2_prior",
        "s.running_kwh_filled",
        "s.second_voltage_distance",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

combined_details = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage_max_2_prior",
        "s.second_voltage",
        "s.running_kwh",
        "s.second_voltage_distance",
        "s.site_installed_kw",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

annotated = (
    combined_details
    .filter(F.col("voltage_max_2_prior") >= 315.0)
    .select(
        "*",
        F.when(F.col("voltage_max_2_prior") >= 630.0, "HIGH").when(F.col("voltage_max_2_prior") >= 446.2, "MEDIUM").otherwise("LOW").alias("voltage_max_2_prior_tier"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_voltage_max_2_prior_tier_sit).alias("rows_in_voltage_max_2_prior_tier_site_id"),
        F.round(F.avg("second_voltage_distance").over(w_all_by_running_kwh_prior_2), 2).alias("second_voltage_distance_avg_2_prior"),
        F.first("site_installed_kw", ignorenulls=True).over(w_all_by_running_kwh_running).alias("first_known_site_installed_kw"),
        F.round(F.col("second_voltage") - F.avg("second_voltage").over(w_voltage_max_2_prior_tier_sit), 2).alias("second_voltage_vs_voltage_max_2_prior_tier_site_"),
        F.nth_value("voltage_max_2_prior", 2).over(w_all_by_running_kwh_full).alias("second_voltage_max_2_prior"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["site_id", "voltage_max_2_prior_tier"], ["site_id"]],
        "site_id", "voltage_max_2_prior_tier",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("voltage_max_2_prior_tier").alias("distinct_voltage_max_2_prior_tiers"),
        F.round(F.sum("second_voltage"), 2).alias("total_second_voltage"),
    )
)
