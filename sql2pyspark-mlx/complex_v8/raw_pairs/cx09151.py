from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_voltage_running = Window.orderBy(F.col("max_voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

site_id_stats = (
    cleaned
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_voltage"))
)

above_average = (
    cleaned.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

sequenced = (
    above_average
    .select(
        "*",
        F.max("max_voltage").over(w_all_by_max_voltage_running).alias("max_max_voltage_so_far"),
        F.sum("kwh").over(w_all_by_max_voltage_running).alias("running_kwh"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.length(F.trim(F.col("quality_prefix"))).alias("quality_prefix_len"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

result = (
    derived
    .groupBy("distinct_site_id_in_quality", "site_id")
    .pivot("quality", ["BAD", "OK"])
    .agg(F.count("voltage_minus_max"))
)
