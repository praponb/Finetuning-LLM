from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 11:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

detail_rows = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 14.5), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.read_at_quarter",
        "s.quality_clean",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

scored = (
    detail_rows
    .filter(F.col("reading_id").isNotNull())
    .select(
        "id",
        "voltage",
        "quality",
        "reading_id",
        F.lower(F.regexp_replace(F.trim(F.col("quality_clean")), " ", "_")).alias("quality_clean_slug"),
        F.round(F.col("read_at_quarter") / F.nullif(F.col("reading_kwh"), F.lit(0)), 3).alias("read_at_quarter_per_reading_kwh"),
        (F.col("voltage") + F.col("read_at_quarter")).alias("voltage_plus_read_at_quarter"),
    )
)

stats = (
    scored
    .groupBy(
        F.when(F.col("voltage") >= 231.7, "large").when(F.col("voltage") >= 219.6, "medium").otherwise("small").alias("voltage_tier"),
        "quality",
        "reading_id",
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("voltage") / F.nullif(F.sum("read_at_quarter_per_reading_kwh"), F.lit(0)), 3).alias("voltage_to_read_at_quarter_per_reading_kwh_ratio"),
        F.round(F.sum("voltage") / F.nullif(F.sum("voltage_plus_read_at_quarter"), F.lit(0)), 3).alias("voltage_to_voltage_plus_read_at_quarter_ratio"),
    )
)

exploded = (
    stats
    .select(
        "voltage_tier",
        "quality",
        "reading_id",
        "voltage_to_read_at_quarter_per_reading_kwh_ratio",
        "voltage_to_voltage_plus_read_at_quarter_ratio",
        F.posexplode("quality_list").alias("quality_list_item_pos", "quality_list_item"),
    )
)

result = (
    exploded
    .select(
        "voltage_tier",
        "reading_id",
        "voltage_to_read_at_quarter_per_reading_kwh_ratio",
        "voltage_to_voltage_plus_read_at_quarter_ratio",
        "quality_list_item_pos",
        "quality_list_item",
        F.concat(F.col("quality"), F.lit("#"), F.col("reading_id").cast("string")).alias("quality_ref"),
    )
    .orderBy(F.col("quality_ref").desc())
)
