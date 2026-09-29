from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
    )
)

filtered = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(15.9, 117.4)), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("kwh").alias("min_reading_kwh"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_kwh", "m.first_reading_read_at")
)

tagged = (
    with_stats
    .select(
        "*",
        F.when(F.col("min_reading_kwh") >= 72.0, "A").when(F.col("min_reading_kwh") >= 24.0, "B").otherwise("C").alias("min_reading_kwh_tier"),
    )
)

result = (
    tagged
    .groupingSets(
        [["quality", "min_reading_kwh_tier", "site_id"], ["quality"], []],
        "quality", "min_reading_kwh_tier", "site_id",
    )
    .agg(
        F.round(F.stddev("kwh_adj"), 2).alias("stddev_kwh_adj"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("min_reading_kwh_tier"), F.lit("ALL")).alias("min_reading_kwh_tier"),
        "site_id",
        "stddev_kwh_adj",
        "quality_list",
        "stddev_voltage",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("min_reading_kwh_tier").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
