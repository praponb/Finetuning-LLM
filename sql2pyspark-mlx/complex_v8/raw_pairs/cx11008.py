from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.when(F.col("kwh") >= 84.9, "large").when(F.col("kwh") >= 33.9, "medium").otherwise("small").alias("kwh_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

kwh_tier_stats = (
    prepared
    .groupBy("kwh_tier")
    .agg(F.max("kwh").alias("max_kwh"))
)

compared = (
    prepared.alias("s")
    .join(kwh_tier_stats.alias("g"), F.col("g.kwh_tier") == F.col("s.kwh_tier"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    compared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage", "m.first_reading_read_at")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at_2"))
)

summarized_rows = (
    with_stats.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at_2")
)

result = (
    summarized_rows
    .groupBy("read_at", "quality_len")
    .pivot("kwh_tier", ["large", "medium", "small"])
    .agg(F.min("kwh"))
)
