from pyspark.sql import functions as F

covered = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_details = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    with_details.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

per_group = (
    with_counts
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("avg_reading_voltage"), 2).alias("stddev_avg_reading_voltage"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    per_group
    .select(
        "read_at_month",
        "cnt",
        F.to_date(F.col("read_at_month")).alias("read_at_month_date"),
    )
    .orderBy(F.col("read_at_month_date").asc(), F.col("read_at_month").desc())
)
