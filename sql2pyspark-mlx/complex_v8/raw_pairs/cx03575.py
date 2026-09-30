from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id")
w_quality = Window.partitionBy("quality")
w_read_at_month_by_site_commis = Window.partitionBy(F.month("read_at")).orderBy("site_commissioned_on")
w_all_by_site_commissioned_on_2 = Window.orderBy("site_commissioned_on")
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id")
w_all_by_site_commissioned_on_3 = Window.orderBy("site_commissioned_on", "id").rowsBetween(-2, -1)

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (~F.col("quality").isin("EST", "BAD")))
)

joined_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_trends = (
    joined_site
    .select(
        "id",
        "site_id",
        "kwh",
        "site_installed_kw",
        F.round(F.percent_rank().over(w_all_by_site_commissioned_on), 3).alias("site_commissioned_on_percent_rank"),
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
        F.round(F.percent_rank().over(w_read_at_month_by_site_commis), 3).alias("site_commissioned_on_percent_rank_2"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.dense_rank().over(w_all_by_site_commissioned_on_2).alias("site_commissioned_on_dense_rank"),
        F.round(F.cume_dist().over(w_quality_by_kwh), 3).alias("kwh_cume_dist"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality), F.lit(0)), 4).alias("site_installed_kw_share_of_quality"),
        F.round(F.avg("site_installed_kw").over(w_all_by_site_commissioned_on_3), 2).alias("site_installed_kw_avg_2_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("site_installed_kw_avg_2_prior") <= 277.2)
    .select(
        "id",
        "site_id",
        "kwh",
        "site_installed_kw",
        "site_commissioned_on_percent_rank",
        "quality_avg_kwh",
        "site_commissioned_on_percent_rank_2",
        "distinct_site_id_in_quality",
        "site_commissioned_on_dense_rank",
        "site_installed_kw_share_of_quality",
        "site_installed_kw_avg_2_prior",
        F.round(F.col("kwh_cume_dist") * 1.1, 2).alias("kwh_cume_dist_adj"),
        (F.col("kwh_cume_dist") - F.col("site_installed_kw")).alias("kwh_cume_dist_minus_site_installed_kw"),
    )
    .orderBy("kwh_cume_dist_adj", "kwh")
)
