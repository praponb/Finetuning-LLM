from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 20.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

unique_rows = (
    enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

profiled = (
    unique_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
        "m.last_reading_read_at",
    )
)

qualified = (
    profiled.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.lower(F.col("si.site_name")).like("%p %")), "left_anti")
)

joined_site = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw").between(141.6, 373.3))
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

result = (
    joined_site
    .select(
        "id",
        "voltage",
        "quality",
        "site_tariff",
        "reading_count",
        "first_reading_read_at",
        "last_reading_read_at",
        "site_installed_kw",
        F.abs(F.col("site_installed_kw") - 373.3).alias("site_installed_kw_distance"),
        F.substring(F.trim(F.col("site_tariff")), 1, 4).alias("site_tariff_prefix"),
    )
    .orderBy(F.col("first_reading_read_at").desc())
)
