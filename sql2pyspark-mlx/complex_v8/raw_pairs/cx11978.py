from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id")

filtered = (
    df_meter_readings
    .filter(F.col("voltage") < 241.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "EST") & (F.col("m.kwh") > 46.6), "left_anti")
)

screened = (
    filtered_2.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(216.1, 243.7)) & (F.col("m.kwh") < 82.9), "left_anti")
    .filter(F.col("quality").isNotNull())
)

joined_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_slug",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_trends = (
    joined_site
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.percent_rank().over(w_all_by_site_commissioned_on), 3).alias("site_commissioned_on_percent_rank"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "quality_slug",
        "site_commissioned_on",
        "site_zone",
        "kwh_share_of_site_id",
        "site_commissioned_on_percent_rank",
        F.last_day(F.col("site_commissioned_on")).alias("site_commissioned_on_month_end"),
        F.lower(F.regexp_replace(F.trim(F.col("quality_slug")), " ", "_")).alias("quality_slug_slug"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.sum("kwh").alias("total_reading_kwh"),
    )
)

with_counts = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.total_reading_kwh",
    )
)

result = (
    with_counts
    .select(
        "id",
        "site_id",
        "quality",
        "quality_slug",
        "site_commissioned_on",
        "site_zone",
        "site_commissioned_on_percent_rank",
        "site_commissioned_on_month_end",
        "quality_slug_slug",
        "reading_count",
        "total_reading_kwh",
        F.quarter(F.col("site_commissioned_on")).alias("site_commissioned_on_quarter"),
        (F.floor(F.col("total_reading_kwh") / 100) * 100).alias("total_reading_kwh_bucket"),
    )
    .orderBy(F.col("id").desc())
)
