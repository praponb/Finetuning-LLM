from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id", "quality").orderBy("voltage_adj", "id")

cleaned = (
    df_meter_readings
    .filter(F.col("kwh") <= 81.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.min("voltage").alias("min_reading_voltage"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.min_reading_voltage")
)

with_activity = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 241.8), "left_anti")
)

unique_rows = (
    with_activity
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

rollup_base = (
    unique_rows
    .filter(F.col("kwh") < 79.4)
    .groupBy(
        "quality",
        F.date_trunc("month", "first_reading_read_at").alias("first_reading_read_at_month"),
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
    )
    .agg(
        F.round(F.sum("min_reading_voltage"), 2).alias("total_min_reading_voltage"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 220.7)
    .drop("sum_voltage_check")
)

result = (
    rollup_base
    .select("quality", "first_reading_read_at_month", "read_at_ym")
    .orderBy(F.col("first_reading_read_at_month").desc(), "quality", "read_at_ym")
    .limit(10)
)
