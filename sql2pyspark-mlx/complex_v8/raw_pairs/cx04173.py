from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_read_at_prior_6 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_quality_site_id_by_kwh_fille = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh_filled").desc(), "id")
w_read_at_month_quality_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy("kwh_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")

base_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled_2"),
    )
)

joined = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.kwh_filled",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_quality_site_id"),
        F.sum("kwh").over(w_all_by_read_at_prior_6).alias("kwh_sum_6_prior"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_site_id_by_kwh_fille))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.first("site_commissioned_on").over(w_read_at_month_quality_by_kwh).alias("first_site_commissioned_on"),
        F.round(F.col("kwh_filled") - F.avg("kwh_filled").over(w_site_id_quality), 2).alias("kwh_filled_vs_site_id_quality_avg"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank"),
        F.min("kwh_filled").over(w_quality_site_id).alias("quality_site_id_min_kwh_filled"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.min("kwh").over(w_site_id_quality).alias("site_id_quality_min_kwh"),
        F.round(F.avg("site_installed_kw").over(w_quality_site_id), 2).alias("quality_site_id_avg_site_installed_kw"),
    )
)

result = (
    windowed
    .filter(F.col("quality_site_id_min_kwh_filled").between(180.0, 1020.0))
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "kwh_filled",
        "site_installed_kw",
        "site_name",
        "site_commissioned_on",
        "kwh_sum_6_prior",
        "minutes_since_prev_read_at",
        "kwh_filled_vs_site_id_quality_avg",
        "read_at_dense_rank",
        "quality_site_id_min_kwh_filled",
        "rows_in_site_id_quality",
        "site_id_quality_min_kwh",
        "quality_site_id_avg_site_installed_kw",
        (F.col("rows_in_site_id_quality") + F.col("site_installed_kw")).alias("rows_in_site_id_quality_plus_site_installed_kw"),
    )
    .orderBy(
        F.col("site_id_quality_min_kwh").desc(),
        F.col("kwh_filled_vs_site_id_quality_avg").asc_nulls_last(),
    )
)
