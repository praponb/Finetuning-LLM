from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 96.9), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.read_at_dayofweek",
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched = (
    with_parent
    .filter(F.col("voltage").between(216.1, 234.7))
    .select(
        "*",
        F.greatest(F.col("read_at_dayofweek"), F.col("voltage")).alias("greatest_read_at_dayofweek_voltage"),
        F.concat_ws(" / ", F.col("quality"), F.col("site_tariff")).alias("quality_site_tariff_label"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

summary = (
    enriched
    .groupBy("site_id", "site_tariff")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("site_tariff") == "EV", 1)) / F.count("*"), 1).alias("pct_site_tariff_ev"),
        F.round(F.sum("quality_len") / F.nullif(F.sum("read_at_dayofweek"), F.lit(0)), 3).alias("quality_len_to_read_at_dayofweek_ratio"),
    )
)

labeled = (
    summary
    .select(
        "site_id",
        "site_tariff",
        "n_rows",
        "pct_site_tariff_ev",
        "quality_len_to_read_at_dayofweek_ratio",
        F.coalesce(F.col("site_tariff"), F.lit("UNKNOWN")).alias("site_tariff_or_unknown"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(42.1, 109.3)), "left_semi")
)

result = (
    filtered
    .select(
        "site_id",
        "n_rows",
        "pct_site_tariff_ev",
        "quality_len_to_read_at_dayofweek_ratio",
        F.substring(F.trim(F.col("site_tariff")), 1, 2).alias("site_tariff_prefix"),
    )
    .orderBy("site_id")
)
