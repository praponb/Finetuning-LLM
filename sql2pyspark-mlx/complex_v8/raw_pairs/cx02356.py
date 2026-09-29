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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

exploded_detail = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["reading_id", "site_id", "reading_quality"], ["reading_id"], ["site_id"]],
        "reading_id", "site_id", "reading_quality",
    )
    .agg(
        F.grouping("reading_id").alias("is_total_reading_id"),
        F.array_sort(F.collect_set("reading_quality")).alias("reading_quality_list"),
    )
)
