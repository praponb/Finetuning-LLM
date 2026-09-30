from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at_running = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_max_kwh_vs_quality_si = Window.orderBy(F.col("max_kwh_vs_quality_site_id_avg").asc_nulls_last(), "quality", "site_id").rowsBetween(-3, -1)
w_quality = Window.partitionBy("quality")
w_all_by_max_kwh_vs_quality_si_2 = Window.orderBy(F.col("max_kwh_vs_quality_site_id_avg").asc_nulls_last(), "quality", "site_id")
w_all_by_max_kwh_vs_quality_si_3 = Window.orderBy(F.col("max_kwh_vs_quality_site_id_avg").asc_nulls_last())

recent = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_ref",
        F.last("kwh", ignorenulls=True).over(w_quality_by_read_at_running).alias("last_known_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.first("kwh").over(w_quality_site_id_by_kwh_full).alias("first_kwh"),
    )
)

summary = (
    metrics
    .filter(F.col("first_kwh") > 16.5)
    .groupBy("quality", "site_id")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum("kwh_vs_quality_site_id_avg").alias("total_kwh_vs_quality_site_id_avg"),
        F.count("*").alias("row_count"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.max("kwh_vs_quality_site_id_avg").alias("max_kwh_vs_quality_site_id_avg"),
        F.sum(F.when(F.col("quality").isin("EST", "BAD"), 1).otherwise(0)).alias("n_quality_in_est_bad"),
    )
)

screened = (
    summary.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

sequenced = (
    screened
    .select(
        "*",
        F.round(F.avg("row_count").over(w_all_by_max_kwh_vs_quality_si), 2).alias("row_count_avg_3_prior"),
        F.round(F.col("distinct_qualitys") - F.avg("distinct_qualitys").over(w_quality), 2).alias("distinct_qualitys_vs_quality_avg"),
        F.round(F.avg("distinct_qualitys").over(w_quality), 2).alias("quality_avg_distinct_qualitys"),
        F.ntile(10).over(w_all_by_max_kwh_vs_quality_si_2).alias("max_kwh_vs_quality_site_id_avg_decile"),
        F.rank().over(w_all_by_max_kwh_vs_quality_si_3).alias("max_kwh_vs_quality_site_id_avg_rank"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_quality), F.lit(0)), 4).alias("row_count_share_of_quality"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isNotNull())
    .select(
        "s.quality",
        "s.site_id",
        "s.quality_list",
        "s.total_kwh_vs_quality_site_id_avg",
        "s.row_count",
        "s.distinct_qualitys",
        "s.distinct_qualitys_vs_quality_avg",
        "s.quality_avg_distinct_qualitys",
        "s.max_kwh_vs_quality_site_id_avg_decile",
        "s.max_kwh_vs_quality_site_id_avg_rank",
        "s.row_count_share_of_quality",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_details
    .select(
        "quality",
        "quality_list",
        "row_count",
        "distinct_qualitys",
        "distinct_qualitys_vs_quality_avg",
        "max_kwh_vs_quality_site_id_avg_rank",
        "row_count_share_of_quality",
        "site_tariff",
        "site_installed_kw",
        "site_zone",
    )
    .orderBy(F.col("row_count").desc())
)
