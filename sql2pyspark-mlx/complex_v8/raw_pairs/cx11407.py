from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group = Window.partitionBy("quality_group")

base_readings = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

enriched = (
    base_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.hour(F.col("m.read_at")).between(6, 8)), "left_anti")
)

enriched_2 = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 141.6)
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

ranked = (
    enriched_2
    .select(
        "*",
        F.max("kwh").over(w_quality_group).alias("quality_group_max_kwh"),
        F.size(F.collect_set("quality").over(w_quality_group)).alias("distinct_quality_in_quality_group"),
    )
)

with_details = (
    ranked.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.site_commissioned_on",
        "s.site_installed_kw",
        "s.quality_group_max_kwh",
        "s.distinct_quality_in_quality_group",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.site_name").alias("site_site_name"),
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

result = (
    with_details
    .groupingSets(
        [["site_tariff", "distinct_quality_in_quality_group", "site_id"], ["site_tariff"], []],
        "site_tariff", "distinct_quality_in_quality_group", "site_id",
    )
    .agg(
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
        F.max("voltage").alias("max_voltage"),
    )
    .orderBy(
        F.col("site_tariff").asc_nulls_last(),
        F.col("distinct_quality_in_quality_group").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
