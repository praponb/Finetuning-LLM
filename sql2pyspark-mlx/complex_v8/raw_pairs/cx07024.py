from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_1d = Window.orderBy(F.col("prior_rows_1d").desc_nulls_first(), "id")
w_all_by_prior_rows_1d_full = Window.orderBy(F.col("prior_rows_1d").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_rows_1d_moving = Window.orderBy(F.col("prior_rows_1d").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_site_id_site_zone = Window.partitionBy("site_id", "site_zone")

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.kwh").alias("prior_kwh_1d"),
    )
)

joined_site = (
    covered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_rows_1d",
        "s.prior_kwh_1d",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

ranked = (
    joined_site
    .select(
        "id",
        "prior_rows_1d",
        "site_zone",
        "site_commissioned_on",
        "site_tariff",
        F.row_number().over(w_all_by_prior_rows_1d).alias("rn"),
        F.nth_value("prior_kwh_1d", 2).over(w_all_by_prior_rows_1d_full).alias("second_prior_kwh_1d"),
        (F.col("prior_rows_1d") - F.lag("prior_rows_1d").over(w_all_by_prior_rows_1d)).alias("prior_rows_1d_change"),
        F.sum("prior_rows_1d").over(w_all_by_prior_rows_1d_moving).alias("prior_rows_1d_sum_7"),
        F.round(F.col("prior_kwh_1d") - F.avg("prior_kwh_1d").over(w_site_id_site_zone), 2).alias("prior_kwh_1d_vs_site_id_site_zone_avg"),
    )
)

site_zone_stats = (
    ranked
    .groupBy("site_zone")
    .agg(F.percentile_approx("prior_rows_1d", 0.5).alias("median_prior_rows_1d"))
)

compared = (
    ranked.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.prior_rows_1d") >= F.col("g.median_prior_rows_1d"))
    .select(
        "s.*",
        "g.median_prior_rows_1d",
        F.round(F.col("s.prior_rows_1d") - F.col("g.median_prior_rows_1d"), 2).alias("prior_rows_1d_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "prior_rows_1d",
        "site_zone",
        "site_commissioned_on",
        "rn",
        "second_prior_kwh_1d",
        "prior_rows_1d_change",
        "prior_kwh_1d_vs_site_id_site_zone_avg",
        F.round(F.col("prior_rows_1d") / F.nullif(F.col("prior_rows_1d_sum_7"), F.lit(0)), 3).alias("prior_rows_1d_per_prior_rows_1d_sum_7"),
        (F.col("rn") + F.col("prior_rows_1d")).alias("rn_plus_prior_rows_1d"),
    )
    .orderBy(
        F.col("prior_kwh_1d_vs_site_id_site_zone_avg").desc(),
        "site_commissioned_on",
        "id",
    )
    .limit(5)
)
