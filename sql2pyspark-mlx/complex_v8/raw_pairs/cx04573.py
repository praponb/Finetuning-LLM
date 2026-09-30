from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("voltage") < 235.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug_2"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

site_id_stats = (
    recent
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

outliers = (
    recent.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(37.0, 113.0)) & (~F.col("m.quality").isin("BAD", "OK")), "left_anti")
    .filter(F.col("avg_voltage") > 213.5)
)

site_id_stats_2 = (
    screened
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage_2"))
)

vs_group = (
    screened.alias("s")
    .join(site_id_stats_2.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage_2"))
    .select(
        "s.*",
        "g.avg_voltage_2",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage_2"), 2).alias("voltage_minus_avg_2"),
    )
)

rollup_base = (
    vs_group
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.datediff(F.max("read_at_date"), F.min("read_at_date")).alias("read_at_date_span_days"),
        F.min("read_at_date").alias("first_read_at_date"),
        F.avg("kwh").alias("avg_kwh_check"),
    )
    .filter(F.col("avg_kwh_check") > 50.4)
    .drop("avg_kwh_check")
)

screened_2 = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("read_at_date_span_days").alias("overall_avg_read_at_date_span_days")))
    .filter(F.col("read_at_date_span_days") > F.col("overall_avg_read_at_date_span_days"))
    .drop("overall_avg_read_at_date_span_days")
)

result = (
    screened_2
    .select(
        "read_at_month",
        "first_read_at",
        "first_read_at_date",
        F.when(F.col("read_at_date_span_days") >= 385, "large").when(F.col("read_at_date_span_days") >= 280, "medium").otherwise("small").alias("read_at_date_span_days_tier"),
    )
    .orderBy(
        F.col("read_at_month").desc(),
        F.col("first_read_at_date").asc_nulls_last(),
    )
)
