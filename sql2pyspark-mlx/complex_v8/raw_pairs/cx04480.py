from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count_site_id = Window.partitionBy("reading_count", "site_id")
w_all_by_reading_count = Window.orderBy("reading_count", "id")

valid = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-06-27").cast("date")) & (~F.col("si.zone").isin("Z3", "Z4")), "left_semi")
)

metrics = (
    with_activity
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "reading_count",
        "reading_count_2",
        F.size(F.collect_set("quality").over(w_reading_count_site_id)).alias("distinct_quality_in_reading_count_site_id"),
        F.count(F.lit(1)).over(w_reading_count_site_id).alias("rows_in_reading_count_site_id"),
        F.max("voltage").over(w_reading_count_site_id).alias("reading_count_site_id_max_voltage"),
        F.ntile(5).over(w_all_by_reading_count).alias("reading_count_quintile"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count_3"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count_3"), F.lit(0)).alias("reading_count_3"),
    )
)

with_details_site = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 284.0)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.reading_count",
        "s.distinct_quality_in_reading_count_site_id",
        "s.rows_in_reading_count_site_id",
        "s.reading_count_site_id_max_voltage",
        "s.reading_count_quintile",
        "s.first_reading_read_at",
        "s.reading_quality_count",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

totals = (
    with_details_site
    .groupBy(
        "reading_count",
        "reading_count_quintile",
        F.year("first_reading_read_at").alias("first_reading_read_at_year"),
    )
    .agg(
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.max("first_reading_read_at").alias("last_first_reading_read_at"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("H%"), F.col("reading_count")).otherwise(0)).alias("reading_count_upper_trim_site_name_like_h"),
    )
)

result = (
    totals
    .select(
        "reading_count",
        "reading_count_quintile",
        "site_name_list",
        "last_first_reading_read_at",
        "distinct_site_ids",
        "reading_count_upper_trim_site_name_like_h",
        F.size(F.col("site_name_list")).alias("site_name_list_count"),
        F.when(F.col("reading_count") >= 6, "A").when(F.col("reading_count") >= 2, "B").otherwise("C").alias("reading_count_tier"),
    )
    .orderBy("reading_count_quintile")
)
