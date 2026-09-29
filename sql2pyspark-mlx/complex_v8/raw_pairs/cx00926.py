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

with_activity = (
    joined_ranges
    .crossJoin(joined_ranges.agg(F.avg("energy_cost").alias("overall_avg_energy_cost")))
    .filter(F.col("energy_cost") > F.col("overall_avg_energy_cost"))
    .drop("overall_avg_energy_cost")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    with_activity.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .filter(F.col("zone").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "energy_cost",
        "reading_count",
        F.round(F.col("reading_count") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("reading_count_per_kwh"),
    )
    .orderBy(F.col("read_at").desc_nulls_first(), F.col("site_id").desc())
)
