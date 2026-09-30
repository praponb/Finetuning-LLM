from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    combined_details.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        "m.avg_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

scored = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff",
        "site_zone",
        "min_reading_voltage",
        "avg_reading_voltage",
        "reading_count",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year_2"),
    )
)

result = (
    scored
    .groupingSets(
        [["reading_count", "quality", "site_id"], ["reading_count"]],
        "reading_count", "quality", "site_id",
    )
    .agg(
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.sum(F.when(F.col("quality").isNotNull(), F.col("min_reading_voltage")).otherwise(0)).alias("min_reading_voltage_quality_is_not_null"),
        F.count("*").alias("n_rows"),
    )
)
