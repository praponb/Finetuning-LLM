from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
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
        F.min("read_at").alias("first_reading_read_at"),
    )
)

profiled = (
    in_window.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

tagged = (
    profiled
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "reading_count",
        "first_reading_read_at",
        F.round(F.col("reading_count") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("reading_count_per_kwh"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "reading_count",
        "first_reading_read_at",
        (F.col("first_reading_read_at") + F.expr("INTERVAL 2 HOURS")).alias("first_reading_read_at_plus_2h"),
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
    )
    .orderBy("zone", "id")
    .limit(10)
)
