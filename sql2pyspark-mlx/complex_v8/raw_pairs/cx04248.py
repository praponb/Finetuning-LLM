from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy("read_at")
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_all_by_read_at_cume_dist = Window.orderBy(F.col("read_at_cume_dist").asc_nulls_last(), "id")

scoped = (
    df_meter_readings
    .filter((F.col("voltage") <= 238.8) & (F.col("kwh") >= 20.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.ntile(5).over(w_site_id_quality_by_read_at).alias("read_at_quintile"),
        F.row_number().over(w_site_id_quality_by_read_at).alias("read_at_seq"),
        F.round(F.cume_dist().over(w_site_id_quality_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.ntile(3).over(w_site_id_quality_by_read_at).alias("read_at_tercile"),
    )
)

enriched_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_quintile",
        "s.read_at_cume_dist",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

summarized_rows = (
    enriched_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

enriched_site_2 = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.read_at_cume_dist",
        "s.site_tariff",
        "s.site_installed_kw",
        "s.last_reading_read_at",
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

with_details = (
    enriched_site_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.upper(F.trim(F.col("si.site_name"))).like("R%"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_cume_dist",
        "s.site_tariff",
        "s.site_installed_kw",
        "s.last_reading_read_at",
        "s.site_tariff_2",
        "si.site_name",
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.round(F.avg("site_installed_kw").over(w_site_id_site_tariff), 2).alias("site_id_site_tariff_avg_site_installed_kw"),
        F.lag("read_at_cume_dist", 1, 0).over(w_all_by_read_at_cume_dist).alias("prev_read_at_cume_dist"),
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id_site_tariff), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id_site_tariff"),
    )
)

result = (
    metrics
    .groupingSets(
        [["site_tariff_2", "site_id", "site_tariff"], ["site_tariff_2"]],
        "site_tariff_2", "site_id", "site_tariff",
    )
    .agg(
        F.grouping("site_tariff_2").alias("is_total_site_tariff_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("H%"), 1).otherwise(0)).alias("n_upper_trim_site_name_like_h"),
        F.round(F.sum("site_installed_kw_share_of_site_id_site_tariff") / F.nullif(F.sum("rows_in_site_id_site_tariff"), F.lit(0)), 3).alias("site_installed_kw_share_of_site_id_site_tariff_t"),
    )
    .select(
        F.coalesce(F.col("site_tariff_2"), F.lit("ALL")).alias("site_tariff_2"),
        "site_id",
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "is_total_site_tariff_2",
        "distinct_ids",
        "n_upper_trim_site_name_like_h",
        "site_installed_kw_share_of_site_id_site_tariff_t",
    )
    .orderBy(
        F.col("site_tariff_2").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("site_tariff").asc_nulls_last(),
    )
)
