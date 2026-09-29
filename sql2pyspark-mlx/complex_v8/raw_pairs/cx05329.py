from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_or_unknown_by_voltag = Window.partitionBy("quality_or_unknown").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown_2"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.size(F.collect_set("quality_or_unknown").over(w_site_id)).alias("distinct_quality_or_unknown_in_site_id"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
        F.last("voltage").over(w_quality_or_unknown_by_voltag).alias("last_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

site_id_stats = (
    windowed
    .groupBy("site_id")
    .agg(F.avg("voltage_share_of_site_id").alias("avg_voltage_share_of_site_id"))
)

above_average = (
    windowed.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage_share_of_site_id") > F.col("g.avg_voltage_share_of_site_id"))
    .select(
        "s.*",
        "g.avg_voltage_share_of_site_id",
        F.round(F.col("s.voltage_share_of_site_id") - F.col("g.avg_voltage_share_of_site_id"), 2).alias("voltage_share_of_site_id_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("voltage") >= 224.8)
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "quality_or_unknown",
        "quality_or_unknown_2",
        "distinct_quality_or_unknown_in_site_id",
        "site_id_max_voltage",
        "last_voltage",
        "voltage_share_of_site_id",
        "rows_in_site_id",
        "avg_voltage_share_of_site_id",
        "voltage_share_of_site_id_minus_avg",
        F.concat_ws("|", F.col("quality_or_unknown"), F.col("quality")).alias("quality_or_unknown_quality_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality_or_unknown_2")), " ", "_")).alias("quality_or_unknown_2_slug"),
    )
    .orderBy("voltage_share_of_site_id")
)
