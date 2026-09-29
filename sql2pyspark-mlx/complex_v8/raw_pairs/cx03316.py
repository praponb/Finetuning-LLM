from pyspark.sql import functions as F

covered = (
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
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    covered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

tagged = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "rate",
        "energy_cost",
        "first_reading_read_at",
        "reading_count",
        F.hour(F.col("first_reading_read_at")).alias("first_reading_read_at_hour"),
        F.substring(F.trim(F.col("zone")), 1, 3).alias("zone_prefix"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

result = (
    tagged
    .filter(F.col("reading_count") < 6)
    .select(
        "id",
        "read_at",
        "rate",
        "energy_cost",
        "first_reading_read_at",
        "first_reading_read_at_hour",
        "zone_prefix",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
    .orderBy(F.col("id").desc(), F.col("energy_cost").asc())
    .limit(10)
)
