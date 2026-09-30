from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_read_at_month_read_at = Window.partitionBy(F.date_trunc("month", "first_read_at"), "read_at_hour").orderBy(F.col("last_read_at").desc())
w_read_at_hour_read_at_month_t = Window.partitionBy("read_at_hour", F.to_date("read_at_month")).orderBy(F.col("first_read_at").desc())
w_read_at_hour_read_at_month_t_2 = Window.partitionBy("read_at_hour", F.to_date("read_at_month")).orderBy(F.col("first_read_at").desc(), "read_at_month", "read_at_hour")
w_first_read_at_month_read_at_2 = Window.partitionBy(F.date_trunc("month", "first_read_at"), "read_at_hour").orderBy(F.col("last_read_at").desc(), "read_at_month", "read_at_hour")

recent = (
    df_meter_readings
    .filter((F.col("voltage") < 241.8) & (F.col("read_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

labeled = (
    recent
    .filter(F.col("voltage").between(219.6, 232.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "days_since_read_at",
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.when(F.col("days_since_read_at") >= 595, "A").when(F.col("days_since_read_at") >= 210, "B").otherwise("C").alias("days_since_read_at_tier"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

with_activity = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("OK", "EST")) & (F.col("m.read_at") >= F.lit("2025-02-06 14:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("days_since_read_at_tier") == "A")
)

summary = (
    with_activity
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "read_at_hour")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.max("read_at").alias("last_read_at"),
        F.percentile_approx("voltage_bucket", 0.5).alias("median_voltage_bucket"),
        F.max("read_at_hour").alias("max_read_at_hour_check"),
    )
    .filter(F.col("max_read_at_hour_check") > 8)
    .drop("max_read_at_hour_check")
)

with_trends = (
    summary
    .select(
        "read_at_month",
        "read_at_hour",
        "first_read_at",
        "last_read_at",
        F.dense_rank().over(w_first_read_at_month_read_at).alias("last_read_at_dense_rank"),
        F.dense_rank().over(w_read_at_hour_read_at_month_t).alias("first_read_at_dense_rank"),
        (F.col("median_voltage_bucket") - F.lag("median_voltage_bucket").over(w_read_at_hour_read_at_month_t_2)).alias("median_voltage_bucket_change"),
        F.round(F.percent_rank().over(w_first_read_at_month_read_at_2), 3).alias("last_read_at_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "read_at_month",
        "read_at_hour",
        "last_read_at",
        "last_read_at_dense_rank",
        "first_read_at_dense_rank",
        "median_voltage_bucket_change",
        F.coalesce(F.col("last_read_at_dense_rank"), F.lit(0)).alias("last_read_at_dense_rank_filled"),
    )
    .orderBy(F.col("last_read_at").desc_nulls_first(), "read_at_month", "read_at_hour")
    .limit(20)
)
