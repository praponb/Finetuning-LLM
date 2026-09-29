from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_prior_2 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)
w_site_tariff = Window.partitionBy("site_tariff")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_quality_or_unknown = Window.partitionBy("distinct_quality_or_unknown_in_site_tariff")
w_reading_quality_by_reading_r = Window.partitionBy("reading_quality").orderBy("reading_read_at", "id", "reading_id")
w_quality_or_unknown_by_readin = Window.partitionBy("quality_or_unknown").orderBy(F.col("reading_read_at").desc(), "id", "reading_id")
w_reading_quality_distinct_qua = Window.partitionBy("reading_quality", "distinct_quality_or_unknown_in_site_tariff").orderBy("reading_voltage_share_of_distinct_quality_or_unk", "id", "reading_id")
w_reading_quality_distinct_qua_2 = Window.partitionBy("reading_quality", "distinct_quality_or_unknown_in_site_tariff").orderBy("reading_voltage_share_of_distinct_quality_or_unk")

valid_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 229.1) & (F.col("site_id") <= 8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

joined = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "FLAT")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_or_unknown",
        "s.read_at_label",
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_or_unknown",
        F.round(F.avg("kwh").over(w_site_id_by_read_at_prior_2), 2).alias("kwh_avg_2_prior"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_tariff), F.lit(0)), 4).alias("kwh_share_of_site_tariff"),
        F.size(F.collect_set("quality_or_unknown").over(w_site_tariff)).alias("distinct_quality_or_unknown_in_site_tariff"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff), 2).alias("kwh_vs_site_tariff_avg"),
        F.last("kwh").over(w_site_id_by_read_at_full).alias("last_kwh"),
    )
)

detail_rows_readings = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_or_unknown",
        "s.distinct_quality_or_unknown_in_site_tariff",
        "s.kwh_vs_site_tariff_avg",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

windowed = (
    detail_rows_readings
    .select(
        "*",
        F.round(F.col("reading_voltage") / F.nullif(F.sum("reading_voltage").over(w_distinct_quality_or_unknown), F.lit(0)), 4).alias("reading_voltage_share_of_distinct_quality_or_unk"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_distinct_quality_or_unknown), F.lit(0)), 4).alias("kwh_share_of_distinct_quality_or_unknown_in_site"),
        F.ntile(4).over(w_reading_quality_by_reading_r).alias("reading_read_at_quartile"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_or_unknown",
        "distinct_quality_or_unknown_in_site_tariff",
        "kwh_vs_site_tariff_avg",
        "reading_id",
        "reading_voltage",
        "reading_read_at",
        "reading_quality",
        "reading_voltage_share_of_distinct_quality_or_unk",
        "kwh_share_of_distinct_quality_or_unknown_in_site",
        "reading_read_at_quartile",
        F.lag("read_at", 3).over(w_quality_or_unknown_by_readin).alias("prev3_read_at"),
        F.dense_rank().over(w_reading_quality_distinct_qua).alias("reading_voltage_share_of_distinct_quality_or_unk_2"),
        F.round(F.percent_rank().over(w_reading_quality_distinct_qua_2), 3).alias("reading_voltage_share_of_distinct_quality_or_unk_3"),
    )
)

totals = (
    windowed_2
    .filter(F.col("site_id") <= 2)
    .groupBy("distinct_quality_or_unknown_in_site_tariff")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("kwh_share_of_distinct_quality_or_unknown_in_site"), 2).alias("stddev_kwh_share_of_distinct_quality_or_unknown_"),
    )
)

result = (
    totals
    .filter(F.col("distinct_quality_or_unknown_in_site_tariff") < 8)
    .select(
        "distinct_quality_or_unknown_in_site_tariff",
        "stddev_kwh_share_of_distinct_quality_or_unknown_",
        F.abs(F.col("row_count") - 8).alias("row_count_distance"),
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
    )
)
