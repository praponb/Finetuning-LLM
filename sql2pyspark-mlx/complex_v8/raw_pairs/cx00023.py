from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_quality_by_kwh_moving_1 = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_quality_by_kwh_running = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_quartile_site_id_by_next = Window.partitionBy("kwh_quartile", "site_id").orderBy("next2_read_at")
w_site_id_kwh_quartile = Window.partitionBy("site_id", "kwh_quartile")
w_dedup = Window.partitionBy("quality", "site_id").orderBy(F.col("next2_read_at").desc(), F.col("id").desc())

base_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("quality") != "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_windows = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.ntile(4).over(w_quality_by_kwh).alias("kwh_quartile"),
        F.round(F.avg("kwh").over(w_quality_by_kwh_moving_1), 2).alias("kwh_avg_2"),
        F.last("kwh", ignorenulls=True).over(w_quality_by_kwh_running).alias("last_known_kwh"),
        F.lead("read_at", 2).over(w_quality_by_kwh).alias("next2_read_at"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.rank().over(w_kwh_quartile_site_id_by_next).alias("next2_read_at_rank"),
        F.round(F.col("kwh_quartile") / F.nullif(F.sum("kwh_quartile").over(w_site_id_kwh_quartile), F.lit(0)), 4).alias("kwh_quartile_share_of_site_id_kwh_quartile"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 373.3)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.next2_read_at",
        "s.next2_read_at_rank",
        "s.kwh_quartile_share_of_site_id_kwh_quartile",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

unique_rows = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched_site = (
    unique_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.next2_read_at",
        "s.next2_read_at_rank",
        "s.kwh_quartile_share_of_site_id_kwh_quartile",
        "s.site_commissioned_on",
        "s.site_installed_kw",
        "s.site_tariff",
        "si.site_name",
    )
)

grouped = (
    enriched_site
    .groupBy(
        F.year("site_commissioned_on").alias("site_commissioned_on_year"),
        "site_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.max("site_commissioned_on").alias("last_site_commissioned_on"),
        F.round(F.sum("site_installed_kw") / F.nullif(F.sum("kwh_quartile_share_of_site_id_kwh_quartile"), F.lit(0)), 3).alias("site_installed_kw_to_kwh_quartile_share_of_site_"),
        F.min("kwh_quartile_share_of_site_id_kwh_quartile").alias("min_kwh_quartile_share_of_site_id_kwh_quartile"),
        F.round(F.avg("next2_read_at_rank"), 2).alias("avg_next2_read_at_rank"),
        F.count(F.when(F.upper(F.trim(F.col("site_name"))).like("D%"), 1)).alias("n_upper_trim_site_name_like_d"),
    )
)

result = (
    grouped
    .filter(F.col("n_rows") <= 11)
    .select(
        "site_commissioned_on_year",
        "site_id",
        "n_rows",
        "last_site_commissioned_on",
        "site_installed_kw_to_kwh_quartile_share_of_site_",
        "min_kwh_quartile_share_of_site_id_kwh_quartile",
        "avg_next2_read_at_rank",
        "n_upper_trim_site_name_like_d",
        F.round(F.col("avg_next2_read_at_rank") * 0.9, 2).alias("avg_next2_read_at_rank_adj"),
        F.round(F.col("avg_next2_read_at_rank") * 0.9, 2).alias("avg_next2_read_at_rank_adj_2"),
    )
    .orderBy(F.col("avg_next2_read_at_rank").desc(), "avg_next2_read_at_rank_adj_2")
)
