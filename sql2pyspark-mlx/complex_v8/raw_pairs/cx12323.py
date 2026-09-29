from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.last_reading_read_at",
    )
)

line_level_readings = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("OK", "BAD")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.reading_count",
        "s.last_reading_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

derived = (
    line_level_readings
    .select(
        "id",
        "rate",
        "reading_count",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        F.greatest(F.col("reading_voltage"), F.col("reading_kwh")).alias("greatest_reading_voltage_reading_kwh"),
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
        F.hour(F.col("last_reading_read_at")).alias("last_reading_read_at_hour"),
    )
)

result = (
    derived
    .groupBy("last_reading_read_at_hour", "reading_count")
    .pivot("zone_group", ["group_b", "other", "group_a"])
    .agg(F.count("greatest_reading_voltage_reading_kwh"))
)
