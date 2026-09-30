from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_holder_id_by_end_date_prior = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(-1, -1)
w_end_date_month_by_end_date = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id")
w_status = Window.partitionBy("status")
w_holder_region_end_date_month = Window.partitionBy("holder_region", F.date_trunc("month", "end_date"))
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id")
w_end_date_month_by_end_date_t = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc())
w_all_by_holder_risk_score_ful = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")
w_second_start_date_month_by_p = Window.partitionBy(F.date_trunc("month", "second_start_date")).orderBy("premium")
w_end_date_month_holder_joined = Window.partitionBy(F.month("end_date"), F.month("holder_joined_on")).orderBy(F.col("distinct_status_in_end_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter(F.col("holder_id") <= 14)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

joined_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_slug",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    joined_holder
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "holder_joined_on",
        "holder_region",
        "holder_risk_score",
        F.dense_rank().over(w_holder_id_by_start_date).alias("start_date_dense_rank"),
        F.sum("premium").over(w_holder_id_by_end_date_prior).alias("premium_sum_1_prior"),
        F.lag("start_date").over(w_end_date_month_by_end_date).alias("prev_start_date"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_status), F.lit(0)), 4).alias("holder_risk_score_share_of_status"),
        F.max("premium").over(w_holder_region_end_date_month).alias("holder_region_end_date_month_max_premium"),
        F.lead("end_date", 3).over(w_holder_id_by_end_date).alias("next3_end_date"),
        F.rank().over(w_end_date_month_by_end_date_t).alias("end_date_rank"),
        F.size(F.collect_set("holder_id").over(w_holder_region_end_date_month)).alias("distinct_holder_id_in_holder_region_end_date_mon"),
        F.nth_value("start_date", 2).over(w_all_by_holder_risk_score_ful).alias("second_start_date"),
        F.size(F.collect_set("holder_region").over(w_status)).alias("distinct_holder_region_in_status"),
        F.size(F.collect_set("status").over(w_end_date_year)).alias("distinct_status_in_end_date_year"),
        F.lead("premium").over(w_all_by_holder_risk_score).alias("next_premium"),
        F.count(F.lit(1)).over(w_end_date_year).alias("rows_in_end_date_year"),
    )
)

ranked = (
    windowed
    .select(
        "id",
        "product",
        "start_date",
        "status",
        "holder_joined_on",
        "holder_risk_score",
        "start_date_dense_rank",
        "premium_sum_1_prior",
        "end_date_rank",
        "distinct_holder_id_in_holder_region_end_date_mon",
        "second_start_date",
        "distinct_status_in_end_date_year",
        "next_premium",
        F.dense_rank().over(w_second_start_date_month_by_p).alias("premium_dense_rank"),
        F.last("end_date").over(w_end_date_month_holder_joined).alias("last_end_date"),
    )
)

rollup_base = (
    ranked
    .groupBy(
        "status",
        F.date_trunc("month", "second_start_date").alias("second_start_date_month"),
        "product",
    )
    .agg(
        F.round(F.sum("end_date_rank") / F.nullif(F.sum("holder_risk_score"), F.lit(0)), 3).alias("end_date_rank_to_holder_risk_score_ratio"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.percentile_approx("distinct_holder_id_in_holder_region_end_date_mon", 0.5).alias("median_distinct_holder_id_in_holder_region_end_d"),
        F.round(F.avg("end_date_rank"), 2).alias("avg_end_date_rank"),
        F.max("next_premium").alias("max_next_premium_check"),
    )
    .filter(F.col("max_next_premium_check") > 2033.8)
    .drop("max_next_premium_check")
)

result = (
    rollup_base
    .select(
        "status",
        "product",
        "end_date_rank_to_holder_risk_score_ratio",
        "product_list",
        "median_distinct_holder_id_in_holder_region_end_d",
        "avg_end_date_rank",
        F.round(F.col("avg_end_date_rank") * 0.25, 2).alias("avg_end_date_rank_adj"),
        F.round(F.col("avg_end_date_rank") * 1.1, 2).alias("avg_end_date_rank_adj_2"),
    )
    .orderBy(
        F.col("end_date_rank_to_holder_risk_score_ratio").desc_nulls_first(),
        F.col("median_distinct_holder_id_in_holder_region_end_d").asc(),
    )
)
