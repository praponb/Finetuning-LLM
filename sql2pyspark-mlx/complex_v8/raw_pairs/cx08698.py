from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")

filtered = (
    df_meter_readings
    .filter(F.col("quality") != "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 104.8, "A").when(F.col("kwh") >= 51.0, "B").otherwise("C").alias("kwh_tier"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

profiled = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at", "m.first_reading_read_at")
)

qualified = (
    profiled.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-11-28").cast("date"))) & (F.col("si.installed_kw") < 284.0), "left_semi")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at_2"))
)

with_stats = (
    qualified.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at_2")
)

latest = (
    with_stats
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "kwh_tier",
        "first_reading_read_at",
        F.date_format(F.col("last_reading_read_at"), "yyyy-MM-dd").alias("last_reading_read_at_label"),
    )
    .orderBy(F.col("kwh_tier").asc(), "quality")
)
