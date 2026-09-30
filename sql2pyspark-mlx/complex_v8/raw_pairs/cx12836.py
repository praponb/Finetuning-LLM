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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

per_group = (
    matched
    .groupBy("site_id", "zone")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("rate"), 2).alias("total_rate"),
        F.max("read_at").alias("last_read_at"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("n_quality_in_bad_ok"),
        F.round(F.sum("energy_cost") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("energy_cost_to_kwh_ratio"),
        F.round(100 * F.count(F.when(F.col("quality") != "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
    )
)

filtered = (
    per_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(213.7, 238.8)) & (~F.col("m.quality").isin("EST", "OK")), "left_anti")
)

enriched = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.zone",
        "s.total_rate",
        "s.last_read_at",
        "s.n_quality_in_bad_ok",
        "s.pct_quality_bad",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    enriched
    .select(
        "site_id",
        "total_rate",
        "last_read_at",
        "n_quality_in_bad_ok",
        "pct_quality_bad",
        "site_commissioned_on",
        "site_zone",
        "site_tariff",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
        F.date_trunc("year", F.col("site_commissioned_on")).alias("site_commissioned_on_year"),
    )
    .orderBy(F.col("total_rate").asc())
)
