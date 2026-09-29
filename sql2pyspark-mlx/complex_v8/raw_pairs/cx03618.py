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
        F.min("kwh").alias("min_reading_kwh"),
        F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
        F.count(F.when(F.col("quality") != "OK", 1)).alias("reading_quality_count_2"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_kwh",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count_3"),
        F.count(F.when(F.col("quality") != "EST", 1)).alias("reading_quality_count_4"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count_3"), F.lit(0)).alias("reading_quality_count_3"),
        F.coalesce(F.col("m.reading_quality_count_4"), F.lit(0)).alias("reading_quality_count_4"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "rate",
        "min_reading_kwh",
        "reading_quality_count",
        "reading_quality_count_3",
        "reading_quality_count_4",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.abs(F.col("kwh") - 57.3).alias("kwh_distance"),
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
