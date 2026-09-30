from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_read_at_ful = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_read_at_tie = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy("read_at")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on")).orderBy("site_commissioned_on", "id").rowsBetween(-2, -1)
w_read_at_year_site_commission = Window.partitionBy(F.year("read_at"), F.month("site_commissioned_on"))
w_read_at_month_by_read_at = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy("read_at", "id")
w_read_at_month_by_read_at_pri = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy("read_at", "id").rowsBetween(-3, -1)
w_read_at_year_by_days_since_r = Window.partitionBy(F.year("read_at")).orderBy(F.col("days_since_read_at").desc(), "id").rowsBetween(-1, -1)
w_read_at_rank_by_days_since_r = Window.partitionBy("read_at_rank").orderBy("days_since_read_at_avg_2_prior", "id")
w_read_at_rank_quality_by_volt = Window.partitionBy("read_at_rank", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_read_at_rank_quality = Window.partitionBy("read_at_rank", "quality")

scoped_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 11)) | (F.col("kwh") > 51.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

joined_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 373.3)
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

sequenced = (
    joined_site
    .select(
        "*",
        F.last("site_installed_kw").over(w_read_at_month_by_read_at_ful).alias("last_site_installed_kw"),
        F.rank().over(w_read_at_month_by_read_at_tie).alias("read_at_rank"),
        F.round(F.avg("days_since_read_at").over(w_site_id_site_commissioned_on), 2).alias("days_since_read_at_avg_2_prior"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_year_site_commission), 2).alias("voltage_vs_read_at_year_site_commissioned_on_mon"),
        F.lead("days_since_read_at", 3, 0).over(w_read_at_month_by_read_at).alias("next3_days_since_read_at"),
        F.round(F.avg("kwh").over(w_read_at_month_by_read_at_pri), 2).alias("kwh_avg_3_prior"),
        F.sum("site_installed_kw").over(w_read_at_year_site_commission).alias("read_at_year_site_commissioned_on_month_sum_site"),
        F.lag("voltage").over(w_read_at_month_by_read_at).alias("prev_voltage"),
        F.sum("days_since_read_at").over(w_read_at_year_by_days_since_r).alias("days_since_read_at_sum_1_prior"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "days_since_read_at",
        "site_name",
        "read_at_rank",
        "days_since_read_at_avg_2_prior",
        "voltage_vs_read_at_year_site_commissioned_on_mon",
        "next3_days_since_read_at",
        "kwh_avg_3_prior",
        "read_at_year_site_commissioned_on_month_sum_site",
        "prev_voltage",
        "days_since_read_at_sum_1_prior",
        F.lead("next3_days_since_read_at", 1, 0).over(w_read_at_rank_by_days_since_r).alias("next_next3_days_since_read_at"),
        F.lead("days_since_read_at", 3).over(w_read_at_rank_quality_by_volt).alias("next3_days_since_read_at_2"),
        F.count(F.lit(1)).over(w_read_at_rank_quality).alias("rows_in_read_at_rank_quality"),
        F.round(F.cume_dist().over(w_read_at_rank_by_days_since_r), 3).alias("days_since_read_at_avg_2_prior_cume_dist"),
        F.lead("read_at_rank", 1, 0).over(w_read_at_rank_quality_by_volt).alias("next_read_at_rank"),
        F.ntile(5).over(w_read_at_rank_by_days_since_r).alias("days_since_read_at_avg_2_prior_quintile"),
        F.round(F.col("kwh_avg_3_prior") / F.nullif(F.sum("kwh_avg_3_prior").over(w_read_at_rank_quality), F.lit(0)), 4).alias("kwh_avg_3_prior_share_of_read_at_rank_quality"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_name",
        "days_since_read_at_avg_2_prior",
        "voltage_vs_read_at_year_site_commissioned_on_mon",
        "next3_days_since_read_at",
        "kwh_avg_3_prior",
        "read_at_year_site_commissioned_on_month_sum_site",
        "prev_voltage",
        "days_since_read_at_sum_1_prior",
        "next_next3_days_since_read_at",
        "next3_days_since_read_at_2",
        "rows_in_read_at_rank_quality",
        "days_since_read_at_avg_2_prior_cume_dist",
        "next_read_at_rank",
        "days_since_read_at_avg_2_prior_quintile",
        F.initcap(F.trim(F.col("site_name"))).alias("site_name_clean"),
        F.round(F.col("kwh_avg_3_prior") * 0.9, 2).alias("kwh_avg_3_prior_adj"),
    )
    .orderBy(
        F.col("quality").desc(),
        F.col("days_since_read_at_avg_2_prior_cume_dist").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
