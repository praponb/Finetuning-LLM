from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy(F.col("commissioned_on").asc_nulls_last(), "id")
w_tariff_group_tariff = Window.partitionBy("tariff_group", "tariff")
w_zone_by_commissioned_on_ties = Window.partitionBy("zone").orderBy(F.col("commissioned_on").asc_nulls_last())

base = (
    df_sites
    .filter((F.col("tariff") == "TOU") & (F.col("commissioned_on").isNotNull()))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("tariff").isin("FLAT", "EV"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
        F.when(F.col("installed_kw") >= 386.6, "A").when(F.col("installed_kw") >= 96.9, "B").otherwise("C").alias("installed_kw_tier"),
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
    )
)

ranked = (
    base
    .select(
        "id",
        "site_name",
        "tariff",
        "tariff_group",
        "installed_kw_tier",
        "commissioned_on_label",
        F.lead("installed_kw").over(w_zone_by_commissioned_on).alias("next_installed_kw"),
        F.sum("installed_kw").over(w_tariff_group_tariff).alias("tariff_group_tariff_sum_installed_kw"),
        F.round(F.cume_dist().over(w_zone_by_commissioned_on_ties), 3).alias("commissioned_on_cume_dist"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_zone_by_commissioned_on)).alias("days_since_prev_commissioned_on"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "site_name",
        "tariff",
        "tariff_group",
        "installed_kw_tier",
        "commissioned_on_label",
        "tariff_group_tariff_sum_installed_kw",
        "commissioned_on_cume_dist",
        "days_since_prev_commissioned_on",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.length(F.trim(F.col("tariff_group"))).alias("tariff_group_len"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
)

result = (
    enriched
    .groupingSets(
        [["tariff_or_unknown", "tariff"], ["tariff_or_unknown"], ["tariff"]],
        "tariff_or_unknown", "tariff",
    )
    .agg(
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
    )
    .orderBy(
        F.col("tariff_or_unknown").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
    )
)
