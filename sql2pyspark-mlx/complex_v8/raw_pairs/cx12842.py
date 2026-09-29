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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

summary = (
    in_window
    .filter(F.col("kwh") >= 24.5)
    .groupBy("quality", F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.min("kwh").alias("min_kwh"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.max("read_at").alias("last_read_at"),
        F.round(100 * F.count(F.when(F.col("quality") != "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
    )
)

result = (
    summary
    .select(
        "quality",
        "read_at_day",
        "distinct_site_ids",
        "zone_list",
        "pct_quality_bad",
        F.add_months(F.col("read_at_day"), 12).alias("read_at_day_plus_12m"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)
