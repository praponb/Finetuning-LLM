from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

site_id_stats = (
    matched
    .groupBy("site_id")
    .agg(F.avg("prior_rows_7d").alias("avg_prior_rows_7d"))
)

vs_group = (
    matched.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_7d") > F.col("g.avg_prior_rows_7d"))
    .select(
        "s.*",
        "g.avg_prior_rows_7d",
        F.round(F.col("s.prior_rows_7d") - F.col("g.avg_prior_rows_7d"), 2).alias("prior_rows_7d_minus_avg"),
    )
)

site_id_stats_2 = (
    vs_group
    .groupBy("site_id")
    .agg(F.percentile_approx("prior_rows_7d_minus_avg", 0.5).alias("median_prior_rows_7d_minus_avg"))
)

vs_group_2 = (
    vs_group.alias("s")
    .join(site_id_stats_2.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_7d_minus_avg") >= F.col("g.median_prior_rows_7d_minus_avg"))
    .select(
        "s.*",
        "g.median_prior_rows_7d_minus_avg",
        F.round(F.col("s.prior_rows_7d_minus_avg") - F.col("g.median_prior_rows_7d_minus_avg"), 2).alias("prior_rows_7d_minus_avg_minus_median"),
    )
)

result = (
    vs_group_2
    .filter(F.col("prior_rows_7d") < 4)
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_7d",
        "avg_prior_rows_7d",
        "prior_rows_7d_minus_avg",
        "median_prior_rows_7d_minus_avg",
        F.greatest(F.col("prior_rows_7d"), F.col("prior_rows_7d_minus_avg")).alias("greatest_prior_rows_7d_prior_rows_7d_minus_avg"),
    )
)
