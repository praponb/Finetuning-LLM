from pyspark.sql import functions as F

prepared = (
    df_sites
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 284.0, "HIGH").when(F.col("installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
        F.round(F.col("installed_kw") * 1.07, 2).alias("installed_kw_adj"),
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
    )
)

per_group = (
    prepared
    .groupBy(F.year("commissioned_on_week").alias("commissioned_on_week_year"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("installed_kw_tier").alias("distinct_installed_kw_tiers"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("installed_kw_adj"), 2).alias("avg_installed_kw_adj"),
    )
)

screened = (
    per_group
    .crossJoin(per_group.agg(F.avg("avg_installed_kw_adj").alias("overall_avg_avg_installed_kw_adj")))
    .filter(F.col("avg_installed_kw_adj") > F.col("overall_avg_avg_installed_kw_adj"))
    .drop("overall_avg_avg_installed_kw_adj")
)

result = (
    screened
    .select(
        "commissioned_on_week_year",
        "cnt",
        "distinct_installed_kw_tiers",
        "distinct_ids",
        "avg_installed_kw_adj",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.round(F.col("avg_installed_kw_adj") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("avg_installed_kw_adj_per_cnt"),
    )
    .orderBy(F.col("cnt_bucket").desc(), F.col("distinct_ids").desc_nulls_first())
)
