from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

screened = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left_semi")
)

tagged = (
    screened
    .filter(F.col("voltage") > 216.1)
    .withColumn("quality_quality_ref_label", F.concat_ws("-", F.col("quality"), F.col("quality_ref")))
)

grouped = (
    tagged
    .groupBy(F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("quality").isin("EST", "BAD"), F.col("voltage_minus_kwh"))).alias("voltage_minus_kwh_quality_in_est_bad"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.min("voltage_minus_kwh_quality_in_est_bad").alias("overall_min_voltage_minus_kwh_quality_in_est_bad")))
    .filter(F.col("voltage_minus_kwh_quality_in_est_bad") > F.col("overall_min_voltage_minus_kwh_quality_in_est_bad"))
    .drop("overall_min_voltage_minus_kwh_quality_in_est_bad")
)

scored = (
    with_activity
    .filter(F.col("n_rows").between(4, 10))
    .select(
        "read_at_day",
        "n_rows",
        "voltage_minus_kwh_quality_in_est_bad",
        "distinct_site_ids",
        F.when(F.col("distinct_site_ids") >= 8, "large").when(F.col("distinct_site_ids") >= 5, "medium").otherwise("small").alias("distinct_site_ids_tier"),
        F.quarter(F.col("read_at_day")).alias("read_at_day_quarter"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at_day")).alias("days_since_read_at_day"),
    )
)

distinct_site_ids_tier_stats = (
    scored
    .groupBy("distinct_site_ids_tier")
    .agg(F.max("read_at_day_quarter").alias("max_read_at_day_quarter"))
)

vs_group = (
    scored.alias("s")
    .join(distinct_site_ids_tier_stats.alias("g"), F.col("g.distinct_site_ids_tier") == F.col("s.distinct_site_ids_tier"), "inner")
    .filter(F.col("s.read_at_day_quarter") == F.col("g.max_read_at_day_quarter"))
    .select(
        "s.*",
        "g.max_read_at_day_quarter",
        F.round(F.col("s.read_at_day_quarter") - F.col("g.max_read_at_day_quarter"), 2).alias("read_at_day_quarter_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "read_at_day",
        F.col("voltage_minus_kwh_quality_in_est_bad").alias("voltage_minus_kwh_quality_in_est_bad_v"),
        F.col("max_read_at_day_quarter").alias("max_read_at_day_quarter_v"),
    )
    .unpivot(
        ids=["read_at_day"],
        values=["voltage_minus_kwh_quality_in_est_bad_v", "max_read_at_day_quarter_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("read_at_day", "measure")
)
