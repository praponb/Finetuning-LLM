from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_read_at = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at").desc(), "id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_site_id = Window.partitionBy("site_id")
w_read_at_month_by_read_at_ful = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_read_at_tie = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at").desc())
w_quality_site_commissioned_on = Window.partitionBy("quality", F.year("site_commissioned_on")).orderBy(F.col("read_at").desc(), "id")
w_site_commissioned_on_month_q = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy("read_at", "id")
w_site_tariff_by_read_at = Window.partitionBy("site_tariff").orderBy("read_at", "id")
w_site_tariff_by_read_at_movin = Window.partitionBy("site_tariff").orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_distinct_site_zone_in_site_i = Window.partitionBy("distinct_site_zone_in_site_id", "site_zone").orderBy("read_at_decile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_site_zone_in_site_i_2 = Window.partitionBy("distinct_site_zone_in_site_id", "site_zone").orderBy("read_at_decile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_dense_rank_quality_b = Window.partitionBy("read_at_dense_rank", "quality").orderBy("kwh_share_of_read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_dense_rank_read_at_d = Window.partitionBy("read_at_dense_rank", "read_at_decile")
w_read_at_dense_rank_quality_b_2 = Window.partitionBy("read_at_dense_rank", "quality").orderBy("kwh_share_of_read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_dense_rank_quality_b_3 = Window.partitionBy("read_at_dense_rank", "quality").orderBy("kwh_share_of_read_at_year", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.col("kwh").between(34.6, 106.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

with_parent_site = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "EV")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

windowed = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_adj",
        "site_zone",
        "site_commissioned_on",
        "site_tariff",
        F.ntile(10).over(w_read_at_month_by_read_at).alias("read_at_decile"),
        F.max("kwh").over(w_read_at_year).alias("read_at_year_max_kwh"),
        F.round(F.col("kwh_adj") - F.avg("kwh_adj").over(w_site_id), 2).alias("kwh_adj_vs_site_id_avg"),
        F.size(F.collect_set("site_tariff").over(w_site_id)).alias("distinct_site_tariff_in_site_id"),
        F.max("kwh_adj").over(w_site_id).alias("site_id_max_kwh_adj"),
        F.nth_value("kwh_adj", 2).over(w_read_at_month_by_read_at_ful).alias("second_kwh_adj"),
        F.lag("kwh_adj", 3).over(w_read_at_month_by_read_at).alias("prev3_kwh_adj"),
        F.round(F.avg("kwh_adj").over(w_read_at_year), 2).alias("read_at_year_avg_kwh_adj"),
        F.dense_rank().over(w_read_at_month_by_read_at_tie).alias("read_at_dense_rank"),
        F.lead("kwh_adj").over(w_quality_site_commissioned_on).alias("next_kwh_adj"),
        F.round(F.percent_rank().over(w_site_commissioned_on_month_q), 3).alias("read_at_percent_rank"),
        F.size(F.collect_set("site_zone").over(w_site_id)).alias("distinct_site_zone_in_site_id"),
        F.lead("read_at").over(w_site_tariff_by_read_at).alias("next_read_at"),
        F.max("kwh_adj").over(w_site_tariff_by_read_at_movin).alias("kwh_adj_max_3"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_year), F.lit(0)), 4).alias("kwh_share_of_read_at_year"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.sum("kwh_share_of_read_at_year").over(w_distinct_site_zone_in_site_i).alias("running_kwh_share_of_read_at_year"),
        F.nth_value("kwh_share_of_read_at_year", 2).over(w_distinct_site_zone_in_site_i_2).alias("second_kwh_share_of_read_at_year"),
        F.last("kwh", ignorenulls=True).over(w_read_at_dense_rank_quality_b).alias("last_known_kwh"),
        F.round(F.col("kwh_adj_vs_site_id_avg") - F.avg("kwh_adj_vs_site_id_avg").over(w_read_at_dense_rank_read_at_d), 2).alias("kwh_adj_vs_site_id_avg_vs_read_at_dense_rank_rea"),
        F.nth_value("next_kwh_adj", 2).over(w_read_at_dense_rank_quality_b_2).alias("second_next_kwh_adj"),
        F.round(F.avg("kwh_share_of_read_at_year").over(w_read_at_dense_rank_quality_b_3), 2).alias("kwh_share_of_read_at_year_avg_7"),
    )
)

result = (
    ranked
    .select(
        "id",
        "kwh",
        "quality",
        "kwh_adj",
        "site_zone",
        "site_tariff",
        "read_at_decile",
        "read_at_year_max_kwh",
        "kwh_adj_vs_site_id_avg",
        "distinct_site_tariff_in_site_id",
        "second_kwh_adj",
        "prev3_kwh_adj",
        "read_at_year_avg_kwh_adj",
        "next_kwh_adj",
        "distinct_site_zone_in_site_id",
        "next_read_at",
        "kwh_adj_max_3",
        "kwh_share_of_read_at_year",
        "running_kwh_share_of_read_at_year",
        "kwh_adj_vs_site_id_avg_vs_read_at_dense_rank_rea",
        "second_next_kwh_adj",
        "kwh_share_of_read_at_year_avg_7",
        F.date_trunc("quarter", F.col("next_read_at")).alias("next_read_at_quarter"),
        F.round(F.col("second_next_kwh_adj") / F.nullif(F.col("read_at_percent_rank"), F.lit(0)), 3).alias("second_next_kwh_adj_per_read_at_percent_rank"),
    )
    .orderBy(F.col("kwh_adj_vs_site_id_avg").asc_nulls_last())
)
