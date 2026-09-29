from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    valid_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh", "m.first_reading_read_at")
)

per_group = (
    summarized_rows
    .groupBy("site_id")
    .agg(
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.max("kwh").alias("max_kwh"),
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
    )
)

scored = (
    per_group
    .filter(F.col("max_kwh") < 90.0)
    .withColumn("total_kwh_minus_voltage_to_kwh_ratio", F.col("total_kwh") - F.col("voltage_to_kwh_ratio"))
)

result = (
    scored
    .select(
        "site_id",
        F.col("max_kwh").alias("max_kwh_v"),
        F.col("total_kwh_minus_voltage_to_kwh_ratio").alias("total_kwh_minus_voltage_to_kwh_ratio_v"),
        F.col("voltage_to_kwh_ratio").alias("voltage_to_kwh_ratio_v"),
    )
    .unpivot(
        ids=["site_id"],
        values=["max_kwh_v", "total_kwh_minus_voltage_to_kwh_ratio_v", "voltage_to_kwh_ratio_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .orderBy("site_id", "metric_name")
)
