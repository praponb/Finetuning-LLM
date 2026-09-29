from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_installed_kw = Window.orderBy("site_installed_kw", "id")
w_all_by_site_installed_kw_mov = Window.orderBy("site_installed_kw", "id").rowsBetween(-3, Window.currentRow)
w_quality_by_site_commissioned = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-6, -1)
w_all_by_site_installed_kw_pri = Window.orderBy("site_installed_kw", "id").rowsBetween(-6, -1)
w_reading_count_site_id_by_sit = Window.partitionBy("reading_count", "site_id").orderBy("site_commissioned_on")
w_reading_count_2 = Window.partitionBy("reading_count_2")
w_reading_count_site_id_by_sit_2 = Window.partitionBy("reading_count", "site_id").orderBy("site_commissioned_on", "id")
w_reading_count_2_quality_by_r = Window.partitionBy("reading_count_2", "quality").orderBy("reading_count")

filtered = (
    df_meter_readings
    .filter(F.col("voltage") > 219.6)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_site_installed_kw), 3).alias("site_installed_kw_percent_rank"),
        F.round(F.avg("kwh").over(w_all_by_site_installed_kw_mov), 2).alias("kwh_avg_4"),
        F.round(F.avg("site_installed_kw").over(w_quality_by_site_commissioned), 2).alias("site_installed_kw_avg_6_prior"),
        F.round(F.avg("voltage").over(w_all_by_site_installed_kw_pri), 2).alias("voltage_avg_6_prior"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

metrics = (
    with_stats
    .select(
        "*",
        F.dense_rank().over(w_reading_count_site_id_by_sit).alias("site_commissioned_on_dense_rank"),
        F.count(F.lit(1)).over(w_reading_count_2).alias("rows_in_reading_count_2"),
        F.lag("kwh_avg_4").over(w_reading_count_site_id_by_sit_2).alias("prev_kwh_avg_4"),
        F.round(F.col("reading_count") / F.nullif(F.sum("reading_count").over(w_reading_count_2), F.lit(0)), 4).alias("reading_count_share_of_reading_count_2"),
        F.round(F.cume_dist().over(w_reading_count_2_quality_by_r), 3).alias("reading_count_cume_dist"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.tariff").isin("EV", "TOU")) & (F.col("si.installed_kw") <= 373.3), "left_semi")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.count("*").alias("reading_count_3"),
    )
)

with_stats_2 = (
    screened.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.last_reading_read_at_2",
        F.coalesce(F.col("m.reading_count_3"), F.lit(0)).alias("reading_count_3"),
    )
)

result = (
    with_stats_2
    .filter(~F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "site_installed_kw",
        "site_installed_kw_percent_rank",
        "kwh_avg_4",
        "site_installed_kw_avg_6_prior",
        "reading_count",
        "last_reading_read_at",
        "site_commissioned_on_dense_rank",
        "prev_kwh_avg_4",
        "reading_count_cume_dist",
        "reading_quality_count",
        "last_reading_read_at_2",
        "reading_count_3",
        F.year(F.col("site_commissioned_on")).alias("site_commissioned_on_year"),
        F.to_date(F.col("last_reading_read_at")).alias("last_reading_read_at_date"),
    )
    .orderBy("site_installed_kw_avg_6_prior", "quality")
)
