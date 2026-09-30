from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

tagged = (
    valid_readings
    .filter(F.col("voltage_filled") >= 218.8)
    .withColumn("kwh_filled_tier", F.when(F.col("kwh_filled") >= 90.0, "A").when(F.col("kwh_filled") >= 36.0, "B").otherwise("C"))
)

tagged_2 = (
    tagged
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "*",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

latest = (
    tagged_2
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

combined_details_site = (
    latest.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_filled",
        "s.kwh_filled",
        "s.quality_clean",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_activity = (
    combined_details_site
    .crossJoin(combined_details_site.agg(F.min("site_installed_kw").alias("overall_min_site_installed_kw")))
    .filter(F.col("site_installed_kw") > F.col("overall_min_site_installed_kw"))
    .drop("overall_min_site_installed_kw")
)

with_children = (
    with_activity.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_filled",
        "s.kwh_filled",
        "s.quality_clean",
        "s.site_installed_kw",
        "s.site_commissioned_on",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    with_children
    .filter(F.col("reading_quality").isin("OK", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "kwh_filled",
        "site_installed_kw",
        "site_commissioned_on",
        "reading_id",
        "reading_read_at",
        F.abs(F.col("site_installed_kw") - 284.0).alias("site_installed_kw_distance"),
    )
    .orderBy("kwh_filled")
)
