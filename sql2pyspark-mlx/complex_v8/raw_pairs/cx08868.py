from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_by_site_i = Window.partitionBy("quality_or_unknown").orderBy("site_installed_kw")
w_site_id_read_at_year_by_days = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("days_since_read_at").desc_nulls_first(), "id")
w_quality_or_unknown_by_site_i_2 = Window.partitionBy("quality_or_unknown").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_or_unknown_b = Window.partitionBy("site_id", "quality_or_unknown").orderBy("read_at", "id")
w_quality_or_unknown_site_tari = Window.partitionBy("quality_or_unknown", "site_tariff")
w_all_by_site_installed_kw_pri = Window.orderBy("site_installed_kw", "id").rowsBetween(-1, -1)
w_site_commissioned_on_month = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"))
w_all_by_site_installed_kw_ful = Window.orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 12))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_details_site = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z3"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.days_since_read_at",
        "s.quality_or_unknown",
        "s.site_name",
        "s.site_installed_kw",
        "s.site_tariff",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

profiled = (
    enriched_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_voltage", "m.last_reading_read_at")
)

windowed = (
    profiled
    .select(
        "id",
        "site_id",
        "read_at",
        "days_since_read_at",
        "quality_or_unknown",
        "site_name",
        "site_installed_kw",
        "site_tariff",
        "site_commissioned_on",
        "total_reading_voltage",
        F.rank().over(w_quality_or_unknown_by_site_i).alias("site_installed_kw_rank"),
        F.dense_rank().over(w_site_id_read_at_year_by_days).alias("days_since_read_at_dense_rank"),
        F.last("site_installed_kw").over(w_quality_or_unknown_by_site_i_2).alias("last_site_installed_kw"),
        F.round(F.cume_dist().over(w_site_id_quality_or_unknown_b), 3).alias("read_at_cume_dist"),
        F.lag("total_reading_voltage").over(w_site_id_quality_or_unknown_b).alias("prev_total_reading_voltage"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality_or_unknown_site_tari), F.lit(0)), 4).alias("site_installed_kw_share_of_quality_or_unknown_si"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "read_at",
        "site_name",
        "site_tariff",
        "site_commissioned_on",
        "total_reading_voltage",
        "site_installed_kw_rank",
        "days_since_read_at_dense_rank",
        "read_at_cume_dist",
        "prev_total_reading_voltage",
        "site_installed_kw_share_of_quality_or_unknown_si",
        F.round(F.avg("site_installed_kw_rank").over(w_all_by_site_installed_kw_pri), 2).alias("site_installed_kw_rank_avg_1_prior"),
        F.round(F.col("total_reading_voltage") - F.avg("total_reading_voltage").over(w_site_commissioned_on_month), 2).alias("total_reading_voltage_vs_site_commissioned_on_mo"),
        F.first("read_at_cume_dist").over(w_all_by_site_installed_kw_ful).alias("first_read_at_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "read_at",
        "site_name",
        "site_tariff",
        "total_reading_voltage",
        "site_installed_kw_rank",
        "days_since_read_at_dense_rank",
        "read_at_cume_dist",
        "prev_total_reading_voltage",
        "site_installed_kw_share_of_quality_or_unknown_si",
        "site_installed_kw_rank_avg_1_prior",
        "total_reading_voltage_vs_site_commissioned_on_mo",
        "first_read_at_cume_dist",
    )
    .orderBy(F.col("read_at").asc_nulls_last(), "site_name", "id")
    .limit(20)
)
