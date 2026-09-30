from pyspark.sql import functions as F

matched = (
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

without_matches = (
    matched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") != "Z2") & (F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date")), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.sum("voltage").alias("total_reading_voltage"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.total_reading_voltage",
    )
)

scored = (
    with_counts
    .filter(F.col("total_reading_voltage") <= 1155.0)
    .withColumn("read_at_date", F.to_date(F.col("read_at")))
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "rate",
        "energy_cost",
        "reading_count",
        "total_reading_voltage",
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
    )
    .orderBy(F.col("voltage").desc_nulls_first(), F.col("read_at").asc())
)
