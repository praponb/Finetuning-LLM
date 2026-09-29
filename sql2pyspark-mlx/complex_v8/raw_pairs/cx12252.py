from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
    )
)

with_children_readings = (
    recent_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality") == "OK"), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

filtered = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("m.voltage") >= 216.1), "left_semi")
    .filter(F.col("installed_kw") >= 141.6)
)

rollup_base = (
    filtered
    .groupBy("zone")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
        F.percentile_approx("installed_kw", 0.5).alias("median_installed_kw"),
        F.max("reading_read_at").alias("last_reading_read_at"),
    )
)

scored = (
    rollup_base
    .select(
        "zone",
        "cnt",
        "distinct_reading_ids",
        "median_installed_kw",
        "last_reading_read_at",
        (F.col("last_reading_read_at") + F.expr("INTERVAL 1 DAYS")).alias("last_reading_read_at_plus_1d"),
        F.date_format(F.col("last_reading_read_at"), "yyyy-MM-dd HH:00").alias("last_reading_read_at_label"),
        F.when(F.col("zone").isin("Z2", "Z4"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
    )
)

derived = (
    scored
    .filter(F.col("zone").isin("Z2", "Z4"))
    .select(
        "*",
        F.concat_ws(" / ", F.col("zone_group"), F.col("zone")).alias("zone_group_zone_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_reading_read_at")).alias("days_since_last_reading_read_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_reading_read_at")).alias("days_since_last_reading_read_at_2"),
    )
)

tagged = (
    derived
    .withColumn("zone_group_zone_label_slug", F.lower(F.regexp_replace(F.trim(F.col("zone_group_zone_label")), " ", "_")))
)

result = (
    tagged
    .select(
        "zone",
        F.col("days_since_last_reading_read_at").alias("days_since_last_reading_read_at_v"),
        F.col("days_since_last_reading_read_at_2").alias("m_days_since_last_reading_read_at_2"),
    )
    .unpivot(
        ids=["zone"],
        values=["days_since_last_reading_read_at_v", "m_days_since_last_reading_read_at_2"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
