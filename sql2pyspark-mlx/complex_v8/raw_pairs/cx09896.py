from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_site_id_quality_by_kwh_adj_f = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_kwh_adj = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh_adj").desc_nulls_first(), "id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_quality_by_kwh_adj_2 = Window.partitionBy("site_id", "quality").orderBy("kwh_adj", "id")
w_site_id_by_kwh_adj = Window.partitionBy("site_id").orderBy("kwh_adj", "id")
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_by_kwh_prior_2 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(-2, -1)
w_kwh_dense_rank = Window.partitionBy("kwh_dense_rank")
w_read_at_to_date_by_kwh_per_v = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("kwh_per_voltage_sum_2_prior").asc_nulls_last(), "id")
w_kwh_dense_rank_second_read_a = Window.partitionBy("kwh_dense_rank", F.year("second_read_at")).orderBy(F.col("kwh_dense_rank").desc_nulls_first(), "id")
w_read_at_to_date_by_kwh_per_v_2 = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("kwh_per_voltage_sum_2_prior").asc_nulls_last())

scoped = (
    df_meter_readings
    .filter(F.col("kwh").between(14.5, 81.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.sum("kwh").over(w_read_at_month).alias("read_at_month_sum_kwh"),
        F.round(F.col("kwh_adj") - F.avg("kwh_adj").over(w_read_at_month), 2).alias("kwh_adj_vs_read_at_month_avg"),
        F.nth_value("read_at", 2).over(w_site_id_quality_by_kwh_adj_f).alias("second_read_at"),
        F.round(F.percent_rank().over(w_site_id_quality_by_kwh_adj), 3).alias("kwh_adj_percent_rank"),
        (F.col("kwh_adj") - F.lag("kwh_adj").over(w_site_id_by_kwh)).alias("kwh_adj_change"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.dense_rank().over(w_site_id_quality_by_kwh_adj_2).alias("kwh_adj_dense_rank"),
        F.ntile(5).over(w_site_id_by_kwh_adj).alias("kwh_adj_quintile"),
        F.lag("kwh_adj").over(w_site_id_by_kwh).alias("prev_kwh_adj"),
        F.dense_rank().over(w_site_id_by_kwh_ties).alias("kwh_dense_rank"),
        F.min("kwh_adj").over(w_site_id_quality).alias("site_id_quality_min_kwh_adj"),
        F.sum("kwh_per_voltage").over(w_site_id_by_kwh_prior_2).alias("kwh_per_voltage_sum_2_prior"),
        F.lead("read_at").over(w_site_id_by_kwh).alias("next_read_at"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("kwh_adj_dense_rank") <= 2)
    .drop("kwh_adj_dense_rank")
)

with_trends_2 = (
    top_per_group
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_per_voltage",
        "read_at_month_sum_kwh",
        "kwh_adj_vs_read_at_month_avg",
        "second_read_at",
        "kwh_adj_percent_rank",
        "kwh_adj_change",
        "rows_in_read_at_month",
        "kwh_adj_quintile",
        "prev_kwh_adj",
        "kwh_dense_rank",
        "site_id_quality_min_kwh_adj",
        F.size(F.collect_set("quality").over(w_kwh_dense_rank)).alias("distinct_quality_in_kwh_dense_rank"),
        F.max("kwh_adj_percent_rank").over(w_kwh_dense_rank).alias("kwh_dense_rank_max_kwh_adj_percent_rank"),
        F.dense_rank().over(w_read_at_to_date_by_kwh_per_v).alias("kwh_per_voltage_sum_2_prior_dense_rank"),
        (F.col("prev_kwh_adj") - F.lag("prev_kwh_adj").over(w_kwh_dense_rank_second_read_a)).alias("prev_kwh_adj_change"),
        F.round(F.percent_rank().over(w_read_at_to_date_by_kwh_per_v), 3).alias("kwh_per_voltage_sum_2_prior_percent_rank"),
        F.size(F.collect_set("site_id").over(w_kwh_dense_rank)).alias("distinct_site_id_in_kwh_dense_rank"),
        F.round(F.cume_dist().over(w_read_at_to_date_by_kwh_per_v_2), 3).alias("kwh_per_voltage_sum_2_prior_cume_dist"),
    )
)

stats = (
    with_trends_2
    .groupBy(
        F.when(F.col("prev_kwh_adj_change") >= 126.0, "A").when(F.col("prev_kwh_adj_change") >= -72.0, "B").otherwise("C").alias("prev_kwh_adj_change_tier"),
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.sum(F.when(F.col("quality") == "BAD", F.col("prev_kwh_adj_change")).otherwise(0)).alias("prev_kwh_adj_change_quality_bad"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.max("second_read_at").alias("last_second_read_at"),
        F.max("prev_kwh_adj_change").alias("max_prev_kwh_adj_change_check"),
    )
    .filter(F.col("max_prev_kwh_adj_change_check") > -126.0)
    .drop("max_prev_kwh_adj_change_check")
)

result = (
    stats
    .select(
        "prev_kwh_adj_change_tier",
        "read_at_month",
        "distinct_site_ids",
        "last_second_read_at",
        F.coalesce(F.col("distinct_site_ids"), F.lit(0)).alias("distinct_site_ids_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_second_read_at")).alias("days_since_last_second_read_at"),
    )
    .orderBy(F.col("read_at_month").desc_nulls_first(), "prev_kwh_adj_change_tier")
    .limit(20)
)
