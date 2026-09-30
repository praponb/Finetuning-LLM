from pyspark.sql import functions as F

valid = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

labeled = (
    valid
    .select(
        "id",
        "tariff",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 284.0, "HIGH").when(F.col("installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

result = (
    labeled
    .groupingSets(
        [["installed_kw_tier", "tariff"], ["installed_kw_tier"], ["tariff"], []],
        "installed_kw_tier", "tariff",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(~F.col("tariff").isin("EV", "TOU"), 1)).alias("n_tariff_not_in_ev_tou"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.max("site_name_len").alias("max_site_name_len"),
    )
    .select(
        F.coalesce(F.col("installed_kw_tier"), F.lit("ALL")).alias("installed_kw_tier"),
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "grouping_level",
        "n_tariff_not_in_ev_tou",
        "tariff_list",
        "max_site_name_len",
    )
)
