from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

quality_stats = (
    with_stats
    .groupBy("quality")
    .agg(F.max("reading_quality_count").alias("max_reading_quality_count"))
)

vs_group = (
    with_stats.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.reading_quality_count") == F.col("g.max_reading_quality_count"))
    .select(
        "s.*",
        "g.max_reading_quality_count",
        F.round(F.col("s.reading_quality_count") - F.col("g.max_reading_quality_count"), 2).alias("reading_quality_count_minus_max"),
    )
)

with_details_site = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_hour",
        "s.last_reading_read_at",
        "s.last_reading_read_at_2",
        "s.reading_quality_count",
        "s.max_reading_quality_count",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

enriched_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

totals = (
    enriched_site
    .groupBy(
        "site_id",
        "site_zone",
        F.date_trunc("month", "last_reading_read_at_2").alias("last_reading_read_at_2_month"),
    )
    .agg(
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.count("*").alias("n_rows"),
        F.min("last_reading_read_at_2").alias("first_last_reading_read_at_2"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .filter(F.col("n_rows") >= 2)
)

exploded = (
    totals
    .select(
        "site_id",
        "site_zone",
        "last_reading_read_at_2_month",
        "n_rows",
        "first_last_reading_read_at_2",
        "distinct_qualitys",
        F.posexplode("site_tariff_list").alias("site_tariff_list_item_pos", "site_tariff_list_item"),
    )
)

result = (
    exploded
    .filter(F.col("site_id").isNotNull())
    .select(
        "site_id",
        "site_zone",
        "last_reading_read_at_2_month",
        "n_rows",
        "first_last_reading_read_at_2",
        "distinct_qualitys",
        "site_tariff_list_item_pos",
        "site_tariff_list_item",
    )
    .orderBy(F.col("site_tariff_list_item").asc())
)
