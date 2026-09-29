from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_voltage_tier = Window.partitionBy("voltage_tier")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 3) & (F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.when(F.col("voltage") >= 237.8, "A").when(F.col("voltage") >= 222.6, "B").otherwise("C").alias("voltage_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

filtered = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp")), "left_anti")
)

voltage_tier_stats = (
    filtered
    .groupBy("voltage_tier")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

vs_group = (
    filtered.alias("s")
    .join(voltage_tier_stats.alias("g"), F.col("g.voltage_tier") == F.col("s.voltage_tier"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

metrics = (
    vs_group
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality_clean",
        "voltage_tier",
        "quality_len",
        "voltage_minus_avg",
        F.count(F.lit(1)).over(w_voltage_tier).alias("rows_in_voltage_tier"),
        F.sum("kwh").over(w_voltage_tier).alias("voltage_tier_sum_kwh"),
        (F.col("avg_voltage") - F.lag("avg_voltage").over(w_all_by_voltage)).alias("avg_voltage_change"),
        F.nth_value("kwh", 2).over(w_all_by_voltage_full).alias("second_kwh"),
        F.max("voltage").over(w_voltage_tier).alias("voltage_tier_max_voltage"),
    )
)

result = (
    metrics
    .filter(F.col("avg_voltage_change") > -49.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality_clean",
        "voltage_tier",
        "quality_len",
        "voltage_minus_avg",
        "rows_in_voltage_tier",
        "voltage_tier_sum_kwh",
        "second_kwh",
    )
    .orderBy(F.col("voltage_minus_avg").asc())
)
