from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_sit = Window.partitionBy(F.month("read_at"), "site_id").orderBy("site_installed_kw", "id").rowsBetween(-6, Window.currentRow)
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_read_at_month_by_kwh_running = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_kwh = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_month_by_kwh_prior_6 = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(-6, -1)
w_read_at_month_site_id_by_sit_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_site_id_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("kwh", "id")
w_read_at_month_site_id_by_kwh_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_site_installed_kw_mov = Window.orderBy("site_installed_kw", "id").rowsBetween(-6, Window.currentRow)
w_second_read_at_month = Window.partitionBy(F.month("second_read_at"))
w_all_by_site_installed_kw_buc = Window.orderBy(F.col("site_installed_kw_bucket").desc(), "id")
w_all_by_kwh_cume_dist = Window.orderBy("kwh_cume_dist", "id")

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

ranked = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "kwh",
        "site_installed_kw",
        "site_name",
        F.max("site_installed_kw").over(w_read_at_month_site_id_by_sit).alias("site_installed_kw_max_7"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_month), F.lit(0)), 4).alias("kwh_share_of_read_at_month"),
        F.sum("site_installed_kw").over(w_read_at_month_by_kwh_running).alias("running_site_installed_kw"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.avg("kwh").over(w_read_at_month_by_kwh_prior_6), 2).alias("kwh_avg_6_prior"),
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.nth_value("read_at", 2).over(w_read_at_month_site_id_by_sit_2).alias("second_read_at"),
        F.round(F.cume_dist().over(w_read_at_month_site_id_by_kwh), 3).alias("kwh_cume_dist"),
        F.min("site_installed_kw").over(w_site_id).alias("site_id_min_site_installed_kw"),
        F.lag("site_installed_kw", 3).over(w_read_at_month_by_kwh).alias("prev3_site_installed_kw"),
        F.first("site_installed_kw", ignorenulls=True).over(w_read_at_month_site_id_by_kwh_2).alias("first_known_site_installed_kw"),
        F.round(F.avg("kwh").over(w_read_at_month), 2).alias("read_at_month_avg_kwh"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "site_installed_kw",
        "site_name",
        "site_installed_kw_max_7",
        "kwh_share_of_read_at_month",
        "running_site_installed_kw",
        "minutes_since_prev_read_at",
        "site_id_min_kwh",
        "site_id_avg_kwh",
        "second_read_at",
        "kwh_cume_dist",
        "first_known_site_installed_kw",
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
        (F.floor(F.col("site_installed_kw") / 100) * 100).alias("site_installed_kw_bucket"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "site_installed_kw",
        "site_name",
        "kwh_share_of_read_at_month",
        "running_site_installed_kw",
        "minutes_since_prev_read_at",
        "site_id_min_kwh",
        "site_id_avg_kwh",
        "second_read_at",
        "kwh_cume_dist",
        F.round(F.avg("site_id_min_kwh").over(w_all_by_site_installed_kw_mov), 2).alias("site_id_min_kwh_avg_7"),
        F.round(F.col("kwh_cume_dist") - F.avg("kwh_cume_dist").over(w_second_read_at_month), 2).alias("kwh_cume_dist_vs_second_read_at_month_avg"),
        F.round(F.col("minutes_since_prev_read_at") / F.nullif(F.sum("minutes_since_prev_read_at").over(w_second_read_at_month), F.lit(0)), 4).alias("minutes_since_prev_read_at_share_of_second_read_"),
        F.round(F.cume_dist().over(w_all_by_site_installed_kw_buc), 3).alias("site_installed_kw_bucket_cume_dist"),
        F.lag("site_id_avg_kwh", 1, 0).over(w_all_by_kwh_cume_dist).alias("prev_site_id_avg_kwh"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_installed_kw",
        "kwh_share_of_read_at_month",
        "running_site_installed_kw",
        "minutes_since_prev_read_at",
        "site_id_min_kwh",
        "second_read_at",
        "kwh_cume_dist",
        "site_id_min_kwh_avg_7",
        "kwh_cume_dist_vs_second_read_at_month_avg",
        "site_installed_kw_bucket_cume_dist",
        F.substring(F.trim(F.col("site_name")), 1, 2).alias("site_name_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
    .orderBy(
        F.col("kwh_share_of_read_at_month").desc(),
        F.col("site_installed_kw").desc(),
    )
)
