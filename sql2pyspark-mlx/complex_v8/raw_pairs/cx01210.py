from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref_2"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

enriched_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.upper(F.trim(F.col("si.site_name"))).like("R%"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_ref",
        "s.quality_ref_2",
        "s.read_at_day",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    enriched_site
    .groupingSets(
        [["quality", "site_id", "site_tariff"], ["quality"], []],
        "quality", "site_id", "site_tariff",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("voltage").alias("total_voltage"),
        F.max("kwh").alias("max_kwh"),
        F.max("read_at_day").alias("last_read_at_day"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "gid",
        "total_voltage",
        "max_kwh",
        "last_read_at_day",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("site_tariff").asc_nulls_last(),
    )
)
