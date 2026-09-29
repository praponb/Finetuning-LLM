from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_site_id_by_rea = Window.partitionBy("quality_group", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

screened = (
    base_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(34.6, 109.3)), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_date",
        F.nth_value("read_at", 2).over(w_quality_group_site_id_by_rea).alias("second_read_at"),
    )
)

enriched = (
    windowed
    .filter(F.col("voltage") < 232.7)
    .withColumn("voltage_tier", F.when(F.col("voltage") >= 231.6, "HIGH").when(F.col("voltage") >= 218.2, "MEDIUM").otherwise("LOW"))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

joined = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at_date",
        "s.second_read_at",
        "s.voltage_tier",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined
    .filter(F.col("site_zone") == "Z3")
    .select(
        "id",
        "read_at_date",
        "voltage_tier",
        "site_installed_kw",
        F.when(F.col("site_installed_kw") >= 373.3, "large").when(F.col("site_installed_kw") >= 141.6, "medium").otherwise("small").alias("site_installed_kw_tier"),
        F.quarter(F.col("second_read_at")).alias("second_read_at_quarter"),
    )
    .orderBy(
        F.col("id").asc_nulls_last(),
        F.col("site_installed_kw_tier").desc_nulls_first(),
    )
)
