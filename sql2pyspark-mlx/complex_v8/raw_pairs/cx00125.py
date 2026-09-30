from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("quality").isin("EST", "OK")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

enriched = (
    recent
    .filter(F.col("quality_prefix").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "quality_prefix",
        F.coalesce(F.col("kwh_per_voltage"), F.lit(0.0)).alias("kwh_per_voltage_filled"),
    )
)

screened = (
    enriched
    .crossJoin(enriched.agg(F.min("kwh_per_voltage_filled").alias("overall_min_kwh_per_voltage_filled")))
    .filter(F.col("kwh_per_voltage_filled") > F.col("overall_min_kwh_per_voltage_filled"))
    .drop("overall_min_kwh_per_voltage_filled")
)

derived = (
    screened
    .withColumn("quality_group", F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other"))
)

screened_2 = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 111.9), "left_semi")
)

result = (
    screened_2
    .groupingSets(
        [["quality", "quality_group"], ["quality"]],
        "quality", "quality_group",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("kwh_per_voltage_filled") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("kwh_per_voltage_filled_to_kwh_ratio"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
    )
)
