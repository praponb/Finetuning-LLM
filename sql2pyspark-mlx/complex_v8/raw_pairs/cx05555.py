from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_zone", "site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh").between(15.9, 109.0)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

combined_details_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.days_since_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

site_id_stats = (
    combined_details_site
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

outliers = (
    combined_details_site.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

latest_per_key = (
    outliers
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .cube("site_zone", "site_id")
    .agg(
        F.round(F.avg("days_since_read_at"), 2).alias("avg_days_since_read_at"),
        F.count("*").alias("row_count"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
    .orderBy(F.col("site_zone").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
