from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_site_zone = Window.partitionBy("site_zone")
w_site_id_read_at_year_by_kwh = Window.partitionBy("site_id", F.year("read_at")).orderBy("kwh")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_year_by_kwh_2 = Window.partitionBy("site_id", F.year("read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_site_installed_kw_sha = Window.orderBy("site_installed_kw_share_of_site_zone_lower_site_", "prev2_read_at_ym", "read_at_ym", "site_zone")

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 37.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 101.2, "A").when(F.col("kwh") >= 45.0, "B").otherwise("C").alias("kwh_tier"),
    )
)

with_parent_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

sequenced = (
    with_parent_site
    .select(
        "*",
        F.last("kwh").over(w_site_id_by_kwh_full).alias("last_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_read_at_month), 2).alias("kwh_vs_site_id_read_at_month_avg"),
        F.sum("kwh").over(w_site_zone).alias("site_zone_sum_kwh"),
        F.rank().over(w_site_id_read_at_year_by_kwh).alias("kwh_rank"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
        F.lag("read_at", 2).over(w_all_by_kwh).alias("prev2_read_at"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_read_at_month), 2).alias("voltage_vs_site_id_read_at_month_avg"),
        F.first("site_installed_kw").over(w_all_by_kwh_full).alias("first_site_installed_kw"),
        F.dense_rank().over(w_site_id_read_at_year_by_kwh).alias("kwh_dense_rank"),
        F.last("site_installed_kw").over(w_site_id_read_at_year_by_kwh_2).alias("last_site_installed_kw"),
        F.nth_value("read_at", 2).over(w_all_by_kwh_full).alias("second_read_at"),
        F.count(F.lit(1)).over(w_site_zone).alias("rows_in_site_zone"),
        F.size(F.collect_set("site_id").over(w_site_zone)).alias("distinct_site_id_in_site_zone"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_zone"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("kwh_rank") <= 2)
    .drop("kwh_rank")
)

per_group = (
    first_rows
    .groupBy(
        F.date_format("prev2_read_at", "yyyy-MM").alias("prev2_read_at_ym"),
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
        "site_zone",
    )
    .agg(
        F.sum(F.when(F.lower(F.col("site_name")).like("% a%"), F.col("site_installed_kw_share_of_site_zone")).otherwise(0)).alias("site_installed_kw_share_of_site_zone_lower_site_"),
        F.max("prev2_read_at").alias("last_prev2_read_at"),
        F.max("second_read_at").alias("last_second_read_at"),
    )
)

metrics = (
    per_group
    .select(
        "prev2_read_at_ym",
        "read_at_ym",
        "site_zone",
        "site_installed_kw_share_of_site_zone_lower_site_",
        F.row_number().over(w_all_by_site_installed_kw_sha).alias("site_installed_kw_share_of_site_zone_lower_site__2"),
        F.round(F.cume_dist().over(w_all_by_site_installed_kw_sha), 3).alias("site_installed_kw_share_of_site_zone_lower_site__3"),
    )
)

result = (
    metrics
    .select(
        "prev2_read_at_ym",
        "site_zone",
        "site_installed_kw_share_of_site_zone_lower_site_",
        "site_installed_kw_share_of_site_zone_lower_site__2",
        "site_installed_kw_share_of_site_zone_lower_site__3",
        F.when(F.col("site_zone").isin("Z3", "Z4"), "group_a").when(F.col("site_zone") == "Z2", "group_b").otherwise("other").alias("site_zone_group"),
    )
    .orderBy(
        F.col("prev2_read_at_ym").desc(),
        F.col("site_zone_group").asc_nulls_last(),
    )
)
