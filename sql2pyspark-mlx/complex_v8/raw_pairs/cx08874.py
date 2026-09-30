from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 232.0) & (F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

screened = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "left_anti")
)

enriched_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "si.site_name",
    )
)

filtered = (
    enriched_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.length(F.trim(F.col("si.site_name"))) >= 6), "left_semi")
)

summary = (
    filtered
    .groupBy("quality", "site_id")
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

result = (
    summary
    .filter(F.col("cnt") > 5)
    .select(
        "quality",
        "site_id",
        "cnt",
        "quality_list",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.length(F.trim(F.col("quality"))).alias("quality_len_2"),
    )
    .orderBy("quality", F.col("quality_len").asc(), "site_id")
    .limit(20)
)
