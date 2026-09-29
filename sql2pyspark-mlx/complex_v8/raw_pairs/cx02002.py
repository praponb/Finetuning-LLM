from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_kwh_prior_1 = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(-1, -1)
w_read_at_month_by_kwh_ties = Window.partitionBy(F.month("read_at")).orderBy("kwh")
w_read_at_quarter_year_by_read = Window.partitionBy(F.year("read_at_quarter")).orderBy("read_at_quarter", "id")
w_read_at_quarter_year_by_read_2 = Window.partitionBy(F.year("read_at_quarter")).orderBy("read_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_quarter_year_by_read_3 = Window.partitionBy(F.year("read_at_quarter")).orderBy("read_at_quarter", "id").rowsBetween(-3, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("site_id") <= 8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.round(F.avg("kwh").over(w_read_at_month_by_kwh_prior_1), 2).alias("kwh_avg_1_prior"),
        F.rank().over(w_read_at_month_by_kwh_ties).alias("kwh_rank"),
        F.dense_rank().over(w_read_at_quarter_year_by_read).alias("read_at_quarter_dense_rank"),
        F.max("kwh").over(w_read_at_quarter_year_by_read_2).alias("max_kwh_so_far"),
        F.max("voltage").over(w_read_at_quarter_year_by_read_3).alias("voltage_max_4"),
        F.ntile(10).over(w_read_at_quarter_year_by_read).alias("read_at_quarter_decile"),
    )
)

enriched_site = (
    ranked.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("S%")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.read_at_quarter",
        "s.kwh_avg_1_prior",
        "s.kwh_rank",
        "s.read_at_quarter_dense_rank",
        "s.max_kwh_so_far",
        "s.voltage_max_4",
        "s.read_at_quarter_decile",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_counts = (
    enriched_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

detail_rows_readings = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_quarter",
        "s.kwh_avg_1_prior",
        "s.kwh_rank",
        "s.max_kwh_so_far",
        "s.voltage_max_4",
        "s.read_at_quarter_decile",
        "s.site_installed_kw",
        "s.reading_quality_count",
        "s.reading_count_2",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

enriched = (
    detail_rows_readings
    .filter(F.col("reading_quality_count").between(2, 6))
    .select(
        "id",
        "site_id",
        "read_at_quarter",
        "kwh_avg_1_prior",
        "kwh_rank",
        "voltage_max_4",
        "read_at_quarter_decile",
        "site_installed_kw",
        "reading_quality_count",
        "reading_id",
        "reading_read_at",
        F.abs(F.col("site_installed_kw") - 141.6).alias("site_installed_kw_distance"),
    )
)

exploded_detail_readings = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh_avg_1_prior",
        "s.voltage_max_4",
        "s.reading_quality_count",
        "s.reading_id",
        "s.site_installed_kw_distance",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    exploded_detail_readings
    .select(
        "id",
        "kwh_avg_1_prior",
        "reading_quality_count",
        "reading_id",
        "site_installed_kw_distance",
        "reading_id_2",
        "reading_quality",
        "reading_voltage",
        F.when(F.col("reading_quality").isin("BAD", "OK"), "group_a").when(F.col("reading_quality") == "EST", "group_b").otherwise("other").alias("reading_quality_group"),
    )
    .orderBy(F.col("reading_voltage").desc(), F.col("reading_id_2").asc())
)
