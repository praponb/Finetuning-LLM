from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")) & (F.col("kwh") > 42.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

totals = (
    recent_readings
    .groupBy("quality_len", "site_id")
    .agg(
        F.max("quality_len").alias("max_quality_len"),
        F.round(F.sum("voltage") / F.nullif(F.sum("quality_len"), F.lit(0)), 3).alias("voltage_to_quality_len_ratio"),
        F.round(F.sum("quality_len") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("quality_len_to_kwh_ratio"),
        F.round(F.sum("kwh") / F.nullif(F.sum("quality_len"), F.lit(0)), 3).alias("kwh_to_quality_len_ratio"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 218.4)
    .drop("max_voltage_check")
)

screened = (
    totals.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    screened
    .select(
        "quality_len",
        "site_id",
        "max_quality_len",
        "voltage_to_quality_len_ratio",
        "quality_len_to_kwh_ratio",
        "kwh_to_quality_len_ratio",
        F.coalesce(F.col("max_quality_len"), F.lit(0)).alias("max_quality_len_filled"),
        (F.col("kwh_to_quality_len_ratio") + F.col("max_quality_len")).alias("kwh_to_quality_len_ratio_plus_max_quality_len"),
    )
)
