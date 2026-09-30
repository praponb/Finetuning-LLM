from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy("premium", "id")
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.month("end_date")).orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_by_premium_ad = Window.partitionBy("product", "status").orderBy("premium_adj")
w_holder_id_end_date_month_by_3 = Window.partitionBy("holder_id", F.month("end_date")).orderBy("premium", "id").rowsBetween(-6, -1)
w_product_start_date_year_by_e = Window.partitionBy("product", F.year("start_date")).orderBy("end_date", "id")
w_start_date_month_by_start_da = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_start_date_month_by_start_da_2 = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id")
w_start_date_month_by_start_da_3 = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_status_by_premium_ad_2 = Window.partitionBy("product", "status").orderBy("premium_adj", "id")
w_premium_decile_filled_premiu = Window.partitionBy("premium_decile_filled", "premium_quartile").orderBy("premium", "id")
w_start_date_decile = Window.partitionBy("start_date_decile")
w_premium_decile_filled_premiu_2 = Window.partitionBy("premium_decile_filled", "premium_quartile").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_decile_prior = Window.orderBy("premium_decile", "id").rowsBetween(-3, -1)
w_median_premium_quartile = Window.partitionBy("median_premium_quartile")
w_distinct_ids_row_count_by_me = Window.partitionBy("distinct_ids", "row_count").orderBy("median_premium_quartile", "premium_sum_3_prior_tier", "premium_rank", "premium_decile")
w_premium_sum_3_prior_tier_by = Window.partitionBy("premium_sum_3_prior_tier").orderBy(F.col("distinct_ids").asc_nulls_last(), "premium_sum_3_prior_tier", "premium_rank", "premium_decile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_policies
    .filter(F.col("premium") < 3465.4)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "premium_adj",
        F.count(F.lit(1)).over(w_holder_id_status).alias("rows_in_holder_id_status"),
        F.ntile(4).over(w_holder_id_end_date_month_by).alias("premium_quartile"),
        F.sum("premium_adj").over(w_holder_id_end_date_month_by_2).alias("running_premium_adj"),
        F.round(F.cume_dist().over(w_product_status_by_premium_ad), 3).alias("premium_adj_cume_dist"),
        F.round(F.avg("premium_adj").over(w_holder_id_end_date_month_by_3), 2).alias("premium_adj_avg_6_prior"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_product_start_date_year_by_e)).alias("premium_adj_change"),
        F.size(F.collect_set("product").over(w_holder_id_status)).alias("distinct_product_in_holder_id_status"),
        F.round(F.avg("premium_adj").over(w_start_date_month_by_start_da), 2).alias("premium_adj_avg_6_prior_2"),
        F.ntile(10).over(w_start_date_month_by_start_da_2).alias("start_date_decile"),
        F.first("start_date").over(w_start_date_month_by_start_da_3).alias("first_start_date"),
        F.ntile(10).over(w_holder_id_end_date_month_by).alias("premium_decile"),
        F.ntile(3).over(w_product_status_by_premium_ad_2).alias("premium_adj_tercile"),
    )
)

derived = (
    metrics
    .filter(F.col("premium_decile") >= 2)
    .select(
        "id",
        "premium",
        "premium_adj",
        "rows_in_holder_id_status",
        "premium_quartile",
        "premium_adj_avg_6_prior",
        "premium_adj_change",
        "premium_adj_avg_6_prior_2",
        "start_date_decile",
        "premium_decile",
        F.dayofweek(F.col("first_start_date")).alias("first_start_date_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.coalesce(F.col("premium_decile"), F.lit(0)).alias("premium_decile_filled"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "premium",
        "premium_quartile",
        "premium_adj_avg_6_prior",
        "premium_adj_avg_6_prior_2",
        "start_date_decile",
        "premium_decile",
        F.rank().over(w_premium_decile_filled_premiu).alias("premium_rank"),
        F.round(F.cume_dist().over(w_premium_decile_filled_premiu), 3).alias("premium_cume_dist"),
        F.round(F.col("premium_adj_avg_6_prior_2") - F.avg("premium_adj_avg_6_prior_2").over(w_start_date_decile), 2).alias("premium_adj_avg_6_prior_2_vs_start_date_decile_a"),
        F.last("premium_quartile", ignorenulls=True).over(w_premium_decile_filled_premiu_2).alias("last_known_premium_quartile"),
        F.sum("premium_quartile").over(w_start_date_decile).alias("start_date_decile_sum_premium_quartile"),
        F.sum("premium").over(w_all_by_premium_decile_prior).alias("premium_sum_3_prior"),
    )
)

per_group = (
    with_trends
    .groupBy(
        F.when(F.col("premium_sum_3_prior") >= 10560.0, "A").when(F.col("premium_sum_3_prior") >= 4640.0, "B").otherwise("C").alias("premium_sum_3_prior_tier"),
        "premium_rank",
        "premium_decile",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("premium_quartile", 0.5).alias("median_premium_quartile"),
        F.round(F.sum("start_date_decile_sum_premium_quartile") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("start_date_decile_sum_premium_quartile_to_premiu"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("premium_cume_dist") / F.nullif(F.sum("last_known_premium_quartile"), F.lit(0)), 3).alias("premium_cume_dist_to_last_known_premium_quartile"),
        F.max("premium_cume_dist").alias("max_premium_cume_dist"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_median_premium_quartile), 2).alias("distinct_ids_vs_median_premium_quartile_avg"),
        F.round(F.percent_rank().over(w_distinct_ids_row_count_by_me), 3).alias("median_premium_quartile_percent_rank"),
        F.lead("median_premium_quartile").over(w_distinct_ids_row_count_by_me).alias("next_median_premium_quartile"),
        F.nth_value("max_premium_cume_dist", 2).over(w_premium_sum_3_prior_tier_by).alias("second_max_premium_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "premium_sum_3_prior_tier",
        "premium_rank",
        "premium_decile",
        "row_count",
        "median_premium_quartile",
        "premium_cume_dist_to_last_known_premium_quartile",
        "distinct_ids_vs_median_premium_quartile_avg",
        "median_premium_quartile_percent_rank",
        "next_median_premium_quartile",
        "second_max_premium_cume_dist",
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
        F.upper(F.trim(F.col("premium_sum_3_prior_tier"))).alias("premium_sum_3_prior_tier_clean"),
    )
    .orderBy(F.col("row_count").asc_nulls_last())
)
