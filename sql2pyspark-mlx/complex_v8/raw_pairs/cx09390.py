from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("last_reading_read_at").desc(), F.col("id").desc())

prepared = df_meter_readings.filter(F.col("quality") == "EST")

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

profiled = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.max_reading_voltage", "m.last_reading_read_at")
)

joined_site = (
    profiled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.max_reading_voltage",
        "s.last_reading_read_at",
        F.col("si.zone").alias("site_zone"),
    )
)

enriched_site = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.max_reading_voltage",
        "s.last_reading_read_at",
        "s.site_zone",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone_2"),
    )
)

tagged = (
    enriched_site
    .filter(F.col("max_reading_voltage") > 213.5)
    .select(
        "*",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        (F.col("kwh") - F.col("max_reading_voltage")).alias("kwh_minus_max_reading_voltage"),
        F.round(F.col("site_installed_kw") / F.nullif(F.col("max_reading_voltage"), F.lit(0)), 3).alias("site_installed_kw_per_max_reading_voltage"),
    )
)

latest_per_key = (
    tagged
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

site_zone_2_stats = (
    latest_per_key
    .groupBy("site_zone_2")
    .agg(F.percentile_approx("max_reading_voltage", 0.5).alias("median_max_reading_voltage"))
)

outliers = (
    latest_per_key.alias("s")
    .join(site_zone_2_stats.alias("g"), F.col("g.site_zone_2") == F.col("s.site_zone_2"), "inner")
    .filter(F.col("s.max_reading_voltage") >= F.col("g.median_max_reading_voltage"))
    .select(
        "s.*",
        "g.median_max_reading_voltage",
        F.round(F.col("s.max_reading_voltage") - F.col("g.median_max_reading_voltage"), 2).alias("max_reading_voltage_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "median_max_reading_voltage",
        "site_installed_kw_per_max_reading_voltage",
    )
    .unpivot(
        ids=["id"],
        values=["median_max_reading_voltage", "site_installed_kw_per_max_reading_voltage"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
