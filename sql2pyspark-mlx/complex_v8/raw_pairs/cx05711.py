from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("kwh") > 34.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

stats = (
    valid
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.sum(F.when(F.col("quality") == "EST", F.col("voltage"))).alias("voltage_quality_est"),
        F.round(100 * F.count(F.when(F.col("quality") == "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

result = (
    stats
    .select("site_id", "pct_quality_ok", "avg_kwh")
    .unpivot(
        ids=["site_id"],
        values=["pct_quality_ok", "avg_kwh"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("site_id", "metric_name")
)
