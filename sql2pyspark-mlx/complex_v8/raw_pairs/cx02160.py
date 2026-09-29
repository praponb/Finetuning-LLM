from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 48.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
        F.sum("kwh").alias("total_reading_kwh"),
    )
)

with_counts = (
    filtered_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.total_reading_kwh",
    )
)

qualified = (
    with_counts
    .crossJoin(with_counts.agg(F.min("max_reading_voltage").alias("overall_min_max_reading_voltage")))
    .filter(F.col("max_reading_voltage") > F.col("overall_min_max_reading_voltage"))
    .drop("overall_min_max_reading_voltage")
)

without_matches = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 101.2), "left_anti")
    .filter(F.col("kwh_plus_voltage") <= 310.2)
)

result = (
    without_matches
    .rollup("site_id", "read_at_month")
    .agg(
        F.round(F.stddev("kwh_plus_voltage"), 2).alias("stddev_kwh_plus_voltage"),
        F.round(F.sum("read_at_month") / F.nullif(F.sum("max_reading_voltage"), F.lit(0)), 3).alias("read_at_month_to_max_reading_voltage_ratio"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("read_at_month").asc_nulls_last(),
    )
)
