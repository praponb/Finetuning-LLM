from pyspark.sql import functions as F

scoped_sites = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label_2"),
    )
)

derived = (
    scoped_sites
    .select(
        "*",
        F.concat_ws("-", F.col("commissioned_on_label_2"), F.col("tariff")).alias("commissioned_on_label_2_tariff_label"),
        F.upper(F.trim(F.col("commissioned_on_label"))).alias("commissioned_on_label_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("commissioned_on_label_2")), " ", "_")).alias("commissioned_on_label_2_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        "installed_kw",
        "commissioned_on",
        "commissioned_on_label_2",
        "commissioned_on_label_2_tariff_label",
        "commissioned_on_label_clean",
        F.substring(F.trim(F.col("commissioned_on_label")), 1, 4).alias("commissioned_on_label_prefix"),
        F.round(F.col("installed_kw") * 1.07, 2).alias("installed_kw_adj"),
    )
    .orderBy(
        "commissioned_on_label_prefix",
        F.col("installed_kw_adj").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
