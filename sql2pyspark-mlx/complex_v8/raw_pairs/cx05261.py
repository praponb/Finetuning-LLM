from pyspark.sql import functions as F

scoped = (
    df_sites
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("tariff").isNotNull())
)

enriched = (
    with_activity
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

grouped = (
    enriched
    .filter(F.col("installed_kw") > 93.3)
    .groupBy("tariff_or_unknown", "tariff")
    .agg(
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.max("installed_kw").alias("max_installed_kw"),
    )
)

result = (
    grouped
    .select(
        "tariff_or_unknown",
        "distinct_tariffs",
        "max_installed_kw",
        F.abs(F.col("distinct_tariffs") - 6).alias("distinct_tariffs_distance"),
        F.concat_ws("-", F.col("tariff_or_unknown"), F.col("tariff")).alias("tariff_or_unknown_tariff_label"),
    )
    .orderBy("distinct_tariffs", "max_installed_kw")
)
