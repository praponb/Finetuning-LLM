from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("voltage_tier", "quality").orderBy(F.col("kwh").desc(), F.col("id").desc())

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 235.8, "large").when(F.col("voltage") >= 222.1, "medium").otherwise("small").alias("voltage_tier"),
    )
)

deduped = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_site = (
    deduped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select(
        "s.*",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

site_zone_stats = (
    joined_site
    .groupBy("site_zone")
    .agg(F.max("voltage").alias("max_voltage"))
)

above_average = (
    joined_site.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

result = (
    above_average
    .rollup("voltage_tier", "site_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("read_at").alias("last_read_at"),
    )
    .orderBy(
        F.col("voltage_tier").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
