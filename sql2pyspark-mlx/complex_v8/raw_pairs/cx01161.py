from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_group",
        "s.read_at_month",
        "s.quality_len",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    joined
    .groupingSets(
        [["quality_len", "site_id"], ["quality_len"], []],
        "quality_len", "site_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
