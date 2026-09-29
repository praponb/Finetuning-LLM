from pyspark.sql import functions as F

joined_ranges = (
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
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

derived = (
    profiled
    .filter(F.hour(F.col("read_at")).between(6, 8))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "reading_count",
        "reading_count_2",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        "site_id",
        "zone",
        "rate",
        "energy_cost",
        "reading_count",
        "reading_count_2",
        "zone_len",
        "kwh_adj",
        "zone_slug",
    )
    .orderBy(F.col("kwh_adj").desc(), F.col("reading_count").asc_nulls_last())
)
