from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.kwh").alias("prior_kwh_3d"),
    )
)

summary = (
    in_window
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "site_id",
        "prior_rows_3d",
    )
    .agg(
        F.round(F.stddev("prior_rows_3d"), 2).alias("stddev_prior_rows_3d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("prior_kwh_3d"), 2).alias("stddev_prior_kwh_3d"),
        F.min("prior_kwh_3d").alias("min_prior_kwh_3d"),
        F.round(F.avg("prior_kwh_3d"), 2).alias("avg_prior_kwh_3d"),
    )
)

annotated = (
    summary
    .select(
        "read_at_month",
        "site_id",
        "prior_rows_3d",
        "distinct_ids",
        "stddev_prior_kwh_3d",
        "min_prior_kwh_3d",
        (F.col("read_at_month") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_month_plus_15m"),
    )
)

result = (
    annotated
    .select(
        "read_at_month",
        "prior_rows_3d",
        "distinct_ids",
        "stddev_prior_kwh_3d",
        "min_prior_kwh_3d",
        "read_at_month_plus_15m",
        F.abs(F.col("distinct_ids") - 7).alias("distinct_ids_distance"),
    )
    .orderBy(
        F.col("prior_rows_3d").desc_nulls_first(),
        F.col("stddev_prior_kwh_3d").desc(),
    )
)
