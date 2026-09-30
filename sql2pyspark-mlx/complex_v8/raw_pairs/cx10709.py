from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "EST")) & (F.col("kwh") < 81.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj_2"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

rollup_base = (
    prepared_readings
    .groupBy(F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.percentile_approx("voltage_adj_2", 0.5).alias("median_voltage_adj_2"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.sum("voltage").alias("total_voltage"),
    )
)

result = (
    rollup_base
    .select("read_at_day", "median_voltage_adj_2", "total_voltage")
    .unpivot(
        ids=["read_at_day"],
        values=["median_voltage_adj_2", "total_voltage"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("read_at_day", "metric")
)
