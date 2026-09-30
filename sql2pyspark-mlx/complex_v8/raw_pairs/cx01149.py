from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

per_group = (
    sessionized
    .filter(F.col("new_session_flag") < 1)
    .groupBy(
        F.when(F.col("voltage") >= 238.3, "large").when(F.col("voltage") >= 223.1, "medium").otherwise("small").alias("voltage_tier"),
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.round(F.sum("new_session_flag") / F.nullif(F.sum("least_voltage_kwh"), F.lit(0)), 3).alias("new_session_flag_to_least_voltage_kwh_ratio"),
        F.max("voltage").alias("max_voltage"),
        F.sum(F.when(~F.col("quality").isin("BAD", "OK"), F.col("new_session_flag"))).alias("new_session_flag_quality_not_in_bad_ok"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.round(F.stddev("least_voltage_kwh"), 2).alias("stddev_least_voltage_kwh"),
        F.sum("new_session_flag").alias("sum_new_session_flag_check"),
    )
    .filter(F.col("sum_new_session_flag_check") > 0)
    .drop("sum_new_session_flag_check")
)

result = (
    per_group
    .filter(F.col("distinct_ids").between(4, 8))
    .select(
        "voltage_tier",
        "read_at_month",
        "new_session_flag_quality_not_in_bad_ok",
        "distinct_ids",
        F.coalesce(F.col("stddev_least_voltage_kwh"), F.lit(0.0)).alias("stddev_least_voltage_kwh_filled"),
        F.coalesce(F.col("voltage_tier"), F.lit("UNKNOWN")).alias("voltage_tier_or_unknown"),
    )
    .orderBy(F.col("stddev_least_voltage_kwh_filled").asc(), "distinct_ids")
)
