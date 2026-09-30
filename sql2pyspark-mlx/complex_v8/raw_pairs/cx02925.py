from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at_month"), "site_id")
w_all_by_voltage_moving_3 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, Window.currentRow)
w_reading_id = Window.partitionBy("reading_id")
w_site_id_by_voltage_moving_2 = Window.partitionBy("site_id").orderBy("voltage", "id", "reading_id").rowsBetween(-2, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("site_id") >= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 237.8) & (F.hour(F.col("m.read_at")).between(6, 12)), "left_semi")
    .filter(F.col("kwh") <= 81.1)
)

metrics = (
    filtered
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_month_site_id), 2).alias("kwh_vs_read_at_month_month_site_id_avg"),
        F.sum("voltage").over(w_all_by_voltage_moving_3).alias("voltage_sum_4"),
    )
)

exploded_detail_readings = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_or_unknown",
        "s.read_at_month",
        "s.kwh_vs_read_at_month_month_site_id_avg",
        "s.voltage_sum_4",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_trends = (
    exploded_detail_readings
    .withColumn("reading_id_avg_voltage_sum_4", F.round(F.avg("voltage_sum_4").over(w_reading_id), 2))
    .withColumn("voltage_max_3", F.max("voltage").over(w_site_id_by_voltage_moving_2))
)

site_id_stats = (
    with_trends
    .groupBy("site_id")
    .agg(F.max("reading_id_avg_voltage_sum_4").alias("max_reading_id_avg_voltage_sum_4"))
)

compared = (
    with_trends.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.reading_id_avg_voltage_sum_4") == F.col("g.max_reading_id_avg_voltage_sum_4"))
    .select(
        "s.*",
        "g.max_reading_id_avg_voltage_sum_4",
        F.round(F.col("s.reading_id_avg_voltage_sum_4") - F.col("g.max_reading_id_avg_voltage_sum_4"), 2).alias("reading_id_avg_voltage_sum_4_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_or_unknown",
        "read_at_month",
        "kwh_vs_read_at_month_month_site_id_avg",
        "voltage_sum_4",
        "reading_id",
        "reading_read_at",
        "reading_id_avg_voltage_sum_4",
        "voltage_max_3",
        "reading_id_avg_voltage_sum_4_minus_max",
        (F.col("voltage") - F.col("voltage_sum_4")).alias("voltage_minus_voltage_sum_4"),
        F.substring(F.trim(F.col("quality_or_unknown")), 1, 2).alias("quality_or_unknown_prefix"),
    )
    .orderBy(
        F.col("reading_id_avg_voltage_sum_4_minus_max").desc(),
        "quality_or_unknown",
        "id",
        "reading_id",
    )
    .limit(20)
)
