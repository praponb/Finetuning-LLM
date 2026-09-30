from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "BAD")) & (F.col("voltage") > 216.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.when(F.col("voltage") >= 235.0, "large").when(F.col("voltage") >= 219.5, "medium").otherwise("small").alias("voltage_tier"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_details = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

per_group = (
    with_details
    .groupBy("site_id")
    .agg(
        F.sum("read_at_quarter").alias("total_read_at_quarter"),
        F.round(100 * F.count(F.when(F.lower(F.col("site_name")).like("%ar%"), 1)) / F.count("*"), 1).alias("pct_lower_site_name_like_ar"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.sum("kwh").alias("total_kwh"),
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
    )
)

qualified = (
    per_group.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("total_kwh") < 540.0)
)

result = (
    qualified
    .select(
        "site_id",
        "total_read_at_quarter",
        "pct_lower_site_name_like_ar",
        "site_name_list",
        F.array_join(F.array_sort(F.col("site_name_list")), ",").alias("site_name_list_text"),
    )
    .orderBy(
        F.col("site_name_list_text").desc_nulls_first(),
        F.col("pct_lower_site_name_like_ar").desc(),
    )
)
