from pyspark.sql import functions as F

matched = (
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
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

per_group = (
    matched
    .groupBy(F.to_date("read_at").alias("read_at_day"), "prior_rows_7d", "site_id")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.round(F.stddev("prior_kwh_7d"), 2).alias("stddev_prior_kwh_7d"),
        F.percentile_approx("prior_kwh_7d", 0.5).alias("median_prior_kwh_7d"),
        F.count("*").alias("cnt"),
        F.sum("prior_kwh_7d").alias("sum_prior_kwh_7d_check"),
    )
    .filter(F.col("sum_prior_kwh_7d_check") > 60.0)
    .drop("sum_prior_kwh_7d_check")
)

result = (
    per_group
    .select(
        "read_at_day",
        "stddev_prior_kwh_7d",
        "median_prior_kwh_7d",
        F.greatest(F.col("prior_rows_7d"), F.col("cnt")).alias("greatest_prior_rows_7d_cnt"),
    )
    .orderBy(F.col("median_prior_kwh_7d").desc_nulls_first())
)
