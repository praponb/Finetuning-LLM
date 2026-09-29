from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_quality_not_in_bad_ok_site = Window.partitionBy("n_quality_not_in_bad_ok", "site_id").orderBy(F.col("n_quality_not_in_bad_ok").desc(), "read_at_ym", "site_id")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour_2"),
    )
)

per_group = (
    filtered
    .filter(F.col("quality") == "OK")
    .groupBy(F.date_format("read_at", "yyyy-MM").alias("read_at_ym"), "site_id")
    .agg(
        F.sum(F.when(~F.col("quality").isin("BAD", "OK"), 1).otherwise(0)).alias("n_quality_not_in_bad_ok"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 218.5)
    .drop("max_voltage_check")
)

metrics = (
    per_group
    .select(
        "read_at_ym",
        "site_id",
        "quality_list",
        F.row_number().over(w_n_quality_not_in_bad_ok_site).alias("rn"),
        F.lag("n_quality_not_in_bad_ok", 1, 0).over(w_n_quality_not_in_bad_ok_site).alias("prev_n_quality_not_in_bad_ok"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

profiled = (
    with_activity.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

scored = (
    profiled
    .select(
        "*",
        F.when(F.col("rn") >= 8, "A").when(F.col("rn") >= 3, "B").otherwise("C").alias("rn_tier"),
        (F.col("last_reading_read_at") + F.expr("INTERVAL 30 MINUTES")).alias("last_reading_read_at_plus_30m"),
    )
)

result = (
    scored
    .filter(F.size(F.col("quality_list")) >= 2)
    .select(
        "read_at_ym",
        "site_id",
        "quality_list",
        "rn",
        "prev_n_quality_not_in_bad_ok",
        "last_reading_read_at",
        "rn_tier",
        "last_reading_read_at_plus_30m",
        F.date_trunc("week", F.col("last_reading_read_at")).alias("last_reading_read_at_week"),
    )
    .orderBy(F.col("prev_n_quality_not_in_bad_ok").asc())
)
