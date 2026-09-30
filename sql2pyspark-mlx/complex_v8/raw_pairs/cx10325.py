from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

filtered = (
    prepared_readings
    .crossJoin(prepared_readings.agg(F.avg("voltage_per_kwh").alias("overall_avg_voltage_per_kwh")))
    .filter(F.col("voltage_per_kwh") > F.col("overall_avg_voltage_per_kwh"))
    .drop("overall_avg_voltage_per_kwh")
)

with_parent_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.voltage_per_kwh",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_parent_site
    .groupingSets(
        [["site_zone", "quality", "site_id"], ["site_zone"], []],
        "site_zone", "quality", "site_id",
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.max("read_at").alias("last_read_at"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "quality_list",
        "last_read_at",
    )
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
