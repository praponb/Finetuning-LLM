from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month = Window.partitionBy(F.date_trunc("month", "holder_joined_on"))
w_product_holder_region_by_pre = Window.partitionBy("product", "holder_region").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_holder_j = Window.partitionBy("status", "holder_id").orderBy("holder_joined_on", "id").rowsBetween(-6, -1)
w_product_holder_segment_by_pr = Window.partitionBy("product", "holder_segment").orderBy("premium_adj", "id").rowsBetween(-3, -1)
w_status_holder_id_by_holder_j_2 = Window.partitionBy("status", "holder_id").orderBy("holder_joined_on", "id")
w_product_holder_region_by_pre_2 = Window.partitionBy("product", "holder_region").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, -1)
w_product_holder_segment_by_pr_2 = Window.partitionBy("product", "holder_segment").orderBy("premium_adj", "id")
w_prev3_holder_joined_on_year = Window.partitionBy(F.year("prev3_holder_joined_on"), "holder_joined_on_tercile").orderBy(F.col("rows_in_holder_joined_on_month").desc())
w_all_by_premium_adj_full = Window.orderBy(F.col("premium_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev3_holder_joined_on_year_2 = Window.partitionBy(F.year("prev3_holder_joined_on"), "holder_joined_on_tercile").orderBy(F.col("rows_in_holder_joined_on_month").desc(), "id")
w_holder_joined_on_month_holde = Window.partitionBy(F.month("holder_joined_on"), "holder_region")

cleaned_policys = (
    df_policies
    .filter((F.col("holder_id") >= 8) | (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
    )
)

with_details = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.premium_adj",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_holder_joined_on_month), 2).alias("premium_adj_vs_holder_joined_on_month_avg"),
        F.nth_value("holder_joined_on", 2).over(w_product_holder_region_by_pre).alias("second_holder_joined_on"),
        F.max("premium").over(w_status_holder_id_by_holder_j).alias("premium_max_6_prior"),
        F.max("premium_adj").over(w_product_holder_segment_by_pr).alias("premium_adj_max_3_prior"),
        F.count(F.lit(1)).over(w_holder_joined_on_month).alias("rows_in_holder_joined_on_month"),
        F.ntile(3).over(w_status_holder_id_by_holder_j_2).alias("holder_joined_on_tercile"),
        F.max("premium").over(w_product_holder_region_by_pre_2).alias("premium_max_6_prior_2"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_holder_joined_on_month), F.lit(0)), 4).alias("premium_adj_share_of_holder_joined_on_month"),
        F.lag("premium", 2).over(w_product_holder_segment_by_pr_2).alias("prev2_premium"),
        F.lag("holder_joined_on", 3).over(w_product_holder_segment_by_pr_2).alias("prev3_holder_joined_on"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_joined_on_month), F.lit(0)), 4).alias("premium_share_of_holder_joined_on_month"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.dense_rank().over(w_prev3_holder_joined_on_year).alias("rows_in_holder_joined_on_month_dense_rank"),
        F.last("premium_adj").over(w_all_by_premium_adj_full).alias("last_premium_adj"),
        F.lead("second_holder_joined_on", 2).over(w_prev3_holder_joined_on_year_2).alias("next2_second_holder_joined_on"),
        F.round(F.percent_rank().over(w_prev3_holder_joined_on_year_2), 3).alias("rows_in_holder_joined_on_month_percent_rank"),
        F.round(F.percent_rank().over(w_prev3_holder_joined_on_year), 3).alias("rows_in_holder_joined_on_month_percent_rank_2"),
        F.size(F.collect_set("holder_id").over(w_holder_joined_on_month_holde)).alias("distinct_holder_id_in_holder_joined_on_month_hol"),
        F.max("premium_max_6_prior").over(w_holder_joined_on_month_holde).alias("holder_joined_on_month_holder_region_max_premium"),
    )
)

rollup_base = (
    windowed_2
    .groupBy(
        F.year("next2_second_holder_joined_on").alias("next2_second_holder_joined_on_year"),
    )
    .agg(
        F.sum("rows_in_holder_joined_on_month_dense_rank").alias("total_rows_in_holder_joined_on_month_dense_rank"),
        F.round(F.sum("premium_adj") / F.nullif(F.sum("distinct_holder_id_in_holder_joined_on_month_hol"), F.lit(0)), 3).alias("premium_adj_to_distinct_holder_id_in_holder_join"),
        F.min("next2_second_holder_joined_on").alias("first_next2_second_holder_joined_on"),
        F.sum(F.when(F.col("product").isin("LIFE", "HOME"), 1).otherwise(0)).alias("n_product_in_life_home"),
        F.sum(F.when(F.col("product") == "HOME", F.col("holder_joined_on_month_holder_region_max_premium"))).alias("holder_joined_on_month_holder_region_max_premium_2"),
        F.sum("rows_in_holder_joined_on_month_percent_rank").alias("sum_rows_in_holder_joined_on_month_percent_rank_"),
    )
    .filter(F.col("sum_rows_in_holder_joined_on_month_percent_rank_") > 0.1)
    .drop("sum_rows_in_holder_joined_on_month_percent_rank_")
)

result = (
    rollup_base
    .filter(F.col("premium_adj_to_distinct_holder_id_in_holder_join") > 1.0)
    .select(
        "next2_second_holder_joined_on_year",
        "total_rows_in_holder_joined_on_month_dense_rank",
        "premium_adj_to_distinct_holder_id_in_holder_join",
        "first_next2_second_holder_joined_on",
        "holder_joined_on_month_holder_region_max_premium_2",
    )
    .orderBy(
        F.col("premium_adj_to_distinct_holder_id_in_holder_join").desc_nulls_first(),
        "next2_second_holder_joined_on_year",
    )
    .limit(10)
)
