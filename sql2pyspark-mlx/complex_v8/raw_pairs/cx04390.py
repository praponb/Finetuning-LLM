from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_read_at_to_date = Window.partitionBy(F.to_date("first_read_at"))
w_min_prior_rows_7d_site_id_by = Window.partitionBy("min_prior_rows_7d", "site_id").orderBy(F.col("first_read_at").desc(), "site_id", "prior_rows_7d")

joined_ranges = (
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
    joined_ranges
    .groupBy("site_id", "prior_rows_7d")
    .agg(
        F.round(F.avg("prior_rows_7d"), 2).alias("avg_prior_rows_7d"),
        F.min("read_at").alias("first_read_at"),
        F.min("prior_rows_7d").alias("min_prior_rows_7d"),
        F.round(F.avg("prior_voltage_7d"), 2).alias("avg_prior_voltage_7d"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("prior_rows_7d") / F.nullif(F.sum("prior_voltage_7d"), F.lit(0)), 3).alias("prior_rows_7d_to_prior_voltage_7d_ratio"),
    )
)

metrics = (
    grouped
    .withColumn("first_read_at_to_date_avg_avg_prior_rows_7d", F.round(F.avg("avg_prior_rows_7d").over(w_first_read_at_to_date), 2))
    .withColumn("prev_avg_prior_rows_7d", F.lag("avg_prior_rows_7d").over(w_min_prior_rows_7d_site_id_by))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    metrics.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .select(
        "site_id",
        "prior_rows_7d",
        "first_read_at",
        "min_prior_rows_7d",
        "avg_prior_voltage_7d",
        "last_read_at",
        "prior_rows_7d_to_prior_voltage_7d_ratio",
        "first_read_at_to_date_avg_avg_prior_rows_7d",
        "prev_avg_prior_rows_7d",
        "reading_count",
        F.date_trunc("year", F.col("last_read_at")).alias("last_read_at_year"),
    )
    .orderBy("avg_prior_voltage_7d")
)
