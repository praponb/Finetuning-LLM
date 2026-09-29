from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 224.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 101.2, "HIGH").when(F.col("kwh") >= 34.6, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

derived = (
    cleaned_readings
    .select(
        "*",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("voltage").alias("min_reading_voltage"))
)

with_stats = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage")
)

filtered = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("EST", "OK")), "left_semi")
    .filter(F.col("voltage") <= 240.2)
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "kwh_tier",
        "read_at_label",
        "kwh_per_voltage",
        F.abs(F.col("kwh") - 79.4).alias("kwh_distance"),
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
    )
    .orderBy(F.col("voltage").desc(), F.col("site_id").desc_nulls_first(), "id")
    .limit(10)
)
