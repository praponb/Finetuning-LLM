from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_distinct_site_ids = Window.partitionBy("kwh_tier", "distinct_site_ids")
w_quality_or_unknown_by_distin = Window.partitionBy("quality_or_unknown").orderBy("distinct_site_ids")

recent_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

joined = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

totals = (
    joined
    .groupBy(
        "quality_or_unknown",
        F.when(F.col("kwh") >= 92.5, "HIGH").when(F.col("kwh") >= 34.1, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("S%"), F.col("kwh_filled")).otherwise(0)).alias("kwh_filled_upper_trim_site_name_like_s"),
        F.max("kwh").alias("max_kwh"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.round(F.stddev("kwh_filled"), 2).alias("stddev_kwh_filled"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
    )
)

windowed = (
    totals
    .select(
        "*",
        F.min("stddev_kwh_filled").over(w_kwh_tier_distinct_site_ids).alias("kwh_tier_distinct_site_ids_min_stddev_kwh_filled"),
        F.size(F.collect_set("quality_or_unknown").over(w_kwh_tier_distinct_site_ids)).alias("distinct_quality_or_unknown_in_kwh_tier_distinct"),
        F.round(F.percent_rank().over(w_quality_or_unknown_by_distin), 3).alias("distinct_site_ids_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("kwh_tier_distinct_site_ids_min_stddev_kwh_filled") >= 480.0)
    .select(
        "quality_or_unknown",
        "kwh_tier",
        "read_at_month",
        "max_kwh",
        "distinct_site_ids",
        "stddev_kwh_filled",
        "kwh_tier_distinct_site_ids_min_stddev_kwh_filled",
        "distinct_site_ids_percent_rank",
        F.lower(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
        F.abs(F.col("stddev_voltage") - 110.2).alias("stddev_voltage_distance"),
    )
    .orderBy(F.col("quality_or_unknown").desc(), "kwh_tier", "read_at_month")
    .limit(20)
)
