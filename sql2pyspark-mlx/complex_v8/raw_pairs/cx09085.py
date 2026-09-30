from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(16.5, 90.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"))
)

summarized_rows = (
    prepared_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage")
)

with_details_site = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_filled",
        "s.read_at_date",
        "s.read_at_year",
        "s.avg_reading_voltage",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

result = (
    with_details_site
    .groupingSets(
        [["site_id", "read_at_year", "quality"], ["site_id"], ["read_at_year"]],
        "site_id", "read_at_year", "quality",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("avg_reading_voltage"), 2).alias("avg_avg_reading_voltage"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("read_at_year").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
