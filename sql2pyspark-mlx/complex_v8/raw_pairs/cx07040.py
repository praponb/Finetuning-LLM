from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

stats = (
    valid_readings
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id")
    .agg(
        F.min("voltage").alias("min_voltage"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum(F.when(F.col("quality").isin("BAD", "EST"), F.col("kwh"))).alias("kwh_quality_in_bad_est"),
        F.sum(F.when(~F.col("quality").isin("BAD", "OK"), F.col("days_since_read_at"))).alias("days_since_read_at_quality_not_in_bad_ok"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

with_stats = (
    stats.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.avg_reading_voltage")
)

result = (
    with_stats
    .select(
        "read_at_day",
        "min_voltage",
        "stddev_voltage",
        "quality_list",
        "days_since_read_at_quality_not_in_bad_ok",
        "avg_reading_voltage",
    )
    .orderBy(
        F.col("read_at_day").desc(),
        F.col("days_since_read_at_quality_not_in_bad_ok").desc(),
    )
)
