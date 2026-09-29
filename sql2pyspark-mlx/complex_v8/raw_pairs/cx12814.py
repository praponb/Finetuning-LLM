from pyspark.sql import functions as F

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.voltage").alias("prior_voltage_7d"),
    )
)

grouped = (
    covered
    .groupBy("site_id", "prior_rows_7d")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("prior_rows_7d"), 2).alias("stddev_prior_rows_7d"),
        F.max("read_at").alias("last_read_at"),
        F.sum("prior_rows_7d").alias("total_prior_rows_7d"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    grouped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.avg_reading_kwh",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    profiled
    .select(
        "site_id",
        "prior_rows_7d",
        "row_count",
        "distinct_ids",
        "stddev_prior_rows_7d",
        "last_read_at",
        "total_prior_rows_7d",
        "reading_count",
        "avg_reading_kwh",
        "reading_count_2",
    )
    .orderBy(F.col("distinct_ids").asc(), "reading_count_2")
)
