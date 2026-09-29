from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("OK", "BAD"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

qualified = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") >= 141.6), "left_semi")
)

rollup_base = (
    qualified
    .filter(F.col("site_id") <= 9)
    .groupBy(
        "site_id",
        "quality",
        F.date_format("read_at_date", "yyyy-MM").alias("read_at_date_ym"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
    )
)

result = (
    rollup_base
    .select(
        "site_id",
        "quality",
        F.col("stddev_kwh").alias("stddev_kwh_v"),
        F.col("avg_kwh").alias("avg_kwh_v"),
    )
    .unpivot(
        ids=["site_id", "quality"],
        values=["stddev_kwh_v", "avg_kwh_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
