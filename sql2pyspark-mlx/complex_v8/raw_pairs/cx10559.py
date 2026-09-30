from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_commis = Window.partitionBy(F.month("commissioned_on"), "commissioned_on_dayofweek").orderBy("first_reading_read_at")
w_commissioned_on_month_commis_2 = Window.partitionBy(F.month("commissioned_on"), "commissioned_on_dayofweek").orderBy("first_reading_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff = Window.partitionBy("tariff")
w_commissioned_on_dayofweek_zo = Window.partitionBy("commissioned_on_dayofweek", "zone").orderBy("commissioned_on", "id")
w_commissioned_on_month_commis_3 = Window.partitionBy(F.month("commissioned_on"), "commissioned_on_dayofweek").orderBy("first_reading_read_at", "id")
w_dedup = Window.partitionBy("tariff", "zone").orderBy("reading_count_vs_tariff_avg", "id")
w_all_by_commissioned_on_dayof = Window.orderBy(F.col("commissioned_on_dayofweek").desc())
w_all_by_reading_count_vs_tari = Window.orderBy(F.col("reading_count_vs_tariff_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone = Window.partitionBy("zone")

recent_sites = (
    df_sites
    .filter((~F.col("tariff").isin("TOU", "EV")) & (F.col("installed_kw") <= 386.6))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.dayofweek(F.col("commissioned_on")).alias("commissioned_on_dayofweek"),
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label"),
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    recent_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.rank().over(w_commissioned_on_month_commis).alias("first_reading_read_at_rank"),
        F.min("installed_kw").over(w_commissioned_on_month_commis_2).alias("min_installed_kw_so_far"),
        F.round(F.col("reading_count") - F.avg("reading_count").over(w_tariff), 2).alias("reading_count_vs_tariff_avg"),
        F.lead("reading_count", 1, 0).over(w_commissioned_on_dayofweek_zo).alias("next_reading_count"),
        F.row_number().over(w_commissioned_on_month_commis_3).alias("first_reading_read_at_seq"),
    )
)

latest = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_commissioned_on_dayof), 3).alias("commissioned_on_dayofweek_percent_rank"),
        F.first("min_installed_kw_so_far").over(w_all_by_reading_count_vs_tari).alias("first_min_installed_kw_so_far"),
        F.sum("reading_count_vs_tariff_avg").over(w_zone).alias("zone_sum_reading_count_vs_tariff_avg"),
        F.max("reading_count_vs_tariff_avg").over(w_zone).alias("zone_max_reading_count_vs_tariff_avg"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_counts = (
    with_trends.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

tagged = (
    with_counts
    .filter(F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        "*",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
    )
)

result = (
    tagged
    .groupBy("commissioned_on_dayofweek")
    .pivot("tariff", ["FLAT", "TOU"])
    .agg(F.sum("first_reading_read_at_seq").alias("total"), F.max("first_reading_read_at_seq").alias("max_v"))
    .orderBy("commissioned_on_dayofweek")
)
