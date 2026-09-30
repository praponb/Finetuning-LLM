from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reading_kwh_ties = Window.orderBy("reading_kwh")
w_reading_quality = Window.partitionBy("reading_quality")
w_all_by_reading_kwh = Window.orderBy("reading_kwh", "id", "reading_id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("quality") != "BAD") & (F.col("kwh").between(28.4, 89.8)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

qualified = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") >= 141.6) & (~F.col("si.tariff").isin("FLAT", "EV")), "left_anti")
)

site_id_stats = (
    qualified
    .groupBy("site_id")
    .agg(F.avg("kwh_bucket").alias("avg_kwh_bucket"))
)

above_average = (
    qualified.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh_bucket") > F.col("g.avg_kwh_bucket"))
    .select(
        "s.*",
        "g.avg_kwh_bucket",
        F.round(F.col("s.kwh_bucket") - F.col("g.avg_kwh_bucket"), 2).alias("kwh_bucket_minus_avg"),
    )
)

line_level = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.kwh_bucket",
        "s.avg_kwh_bucket",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_windows = (
    line_level
    .select(
        "id",
        "kwh",
        "reading_id",
        "reading_quality",
        "reading_kwh",
        F.round(F.cume_dist().over(w_all_by_reading_kwh_ties), 3).alias("reading_kwh_cume_dist"),
        F.count(F.lit(1)).over(w_reading_quality).alias("rows_in_reading_quality"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_reading_quality), 2).alias("voltage_vs_reading_quality_avg"),
        F.dense_rank().over(w_all_by_reading_kwh).alias("reading_kwh_dense_rank"),
        F.round(F.col("avg_kwh_bucket") / F.nullif(F.sum("avg_kwh_bucket").over(w_reading_quality), F.lit(0)), 4).alias("avg_kwh_bucket_share_of_reading_quality"),
    )
)

derived = (
    with_windows
    .filter(F.col("reading_kwh") > 14.4)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("reading_quality")), " ", "_")).alias("reading_quality_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("reading_quality")), " ", "_")).alias("reading_quality_slug_2"),
    )
)

result = (
    derived
    .filter(F.col("rows_in_reading_quality").between(2, 19))
    .select(
        "id",
        "reading_quality",
        "reading_kwh",
        "reading_kwh_cume_dist",
        "rows_in_reading_quality",
        "voltage_vs_reading_quality_avg",
        "reading_kwh_dense_rank",
        "avg_kwh_bucket_share_of_reading_quality",
        "reading_quality_slug",
        "reading_quality_slug_2",
        F.lower(F.regexp_replace(F.trim(F.col("reading_quality_slug")), " ", "_")).alias("reading_quality_slug_slug"),
        F.substring(F.trim(F.col("reading_quality_slug")), 1, 4).alias("reading_quality_slug_prefix"),
    )
    .orderBy(
        "voltage_vs_reading_quality_avg",
        F.col("reading_quality_slug_2").desc(),
    )
)
