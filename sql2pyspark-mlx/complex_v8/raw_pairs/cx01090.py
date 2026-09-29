from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 101.2)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 50.4).alias("kwh_distance"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

totals = (
    prepared_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("kwh_distance").alias("max_kwh_distance"),
        F.round(F.sum("voltage") / F.nullif(F.sum("greatest_kwh_voltage"), F.lit(0)), 3).alias("voltage_to_greatest_kwh_voltage_ratio"),
        F.min("greatest_kwh_voltage").alias("min_greatest_kwh_voltage"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.min("kwh_distance").alias("min_kwh_distance"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)).alias("reading_quality_count"),
    )
)

profiled = (
    totals.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    profiled
    .select(
        "site_id",
        "n_rows",
        "max_kwh_distance",
        "voltage_to_greatest_kwh_voltage_ratio",
        "min_greatest_kwh_voltage",
        "n_quality_is_not_null",
        "min_kwh_distance",
        "reading_quality_count",
        F.least(F.col("n_rows"), F.col("n_quality_is_not_null")).alias("least_n_rows_n_quality_is_not_null"),
        F.round(F.col("reading_quality_count") / F.nullif(F.col("max_kwh_distance"), F.lit(0)), 3).alias("reading_quality_count_per_max_kwh_distance"),
    )
    .orderBy(
        F.col("min_kwh_distance").desc_nulls_first(),
        "least_n_rows_n_quality_is_not_null",
    )
)
