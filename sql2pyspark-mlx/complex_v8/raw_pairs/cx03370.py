from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "EST")) & (F.col("kwh") <= 80.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

grouped = (
    scoped
    .filter(F.col("voltage") < 229.1)
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "site_id")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.min("read_at").alias("first_read_at"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
    )
)

with_parent_site = (
    grouped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isin("Z3", "Z4"))
    .select(
        "s.read_at_month",
        "s.site_id",
        "s.quality_list",
        "s.first_read_at",
        "s.last_read_at",
        "s.total_voltage",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

joined_site = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.read_at_month",
        "s.site_id",
        "s.quality_list",
        "s.first_read_at",
        "s.last_read_at",
        "s.total_voltage",
        "s.site_zone",
        "s.site_installed_kw",
        "s.site_tariff",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

result = (
    joined_site
    .groupBy("site_id", "first_read_at")
    .pivot("site_zone", ["Z3", "Z2"])
    .agg(F.min("total_voltage"))
)
