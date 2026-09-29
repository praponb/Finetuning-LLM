from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_holder_id = Window.partitionBy(F.month("end_date"), "holder_id")
w_product_holder_segment_by_pr = Window.partitionBy("product", "holder_segment").orderBy(F.col("premium_bucket").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_product_holder_segment_by_pr_2 = Window.partitionBy("product", "holder_segment").orderBy(F.col("premium_bucket").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment_start_date_ye = Window.partitionBy("holder_segment", F.year("start_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first())
w_start_date_plus_7d_year_by_s = Window.partitionBy(F.year("start_date_plus_7d")).orderBy("start_date", "id")
w_product_holder_segment_by_pr_3 = Window.partitionBy("product", "holder_segment").orderBy(F.col("premium_bucket").desc_nulls_first(), "id")
w_product_start_date_plus_7d_m = Window.partitionBy("product", F.month("start_date_plus_7d"))
w_holder_id_by_end_date_moving = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id")
w_first_end_date_month = Window.partitionBy(F.date_trunc("month", "first_end_date"))
w_all_by_max_running_holder_ri = Window.orderBy(F.col("max_running_holder_risk_score").asc_nulls_last(), "holder_segment", "start_date_ym")

recent_policys = (
    df_policies
    .filter(F.col("holder_id") <= 12)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.date_add(F.col("start_date"), 7).alias("start_date_plus_7d"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_parent_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

ranked = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "premium_bucket",
        "holder_risk_score",
        "holder_segment",
        F.size(F.collect_set("holder_segment").over(w_end_date_month_holder_id)).alias("distinct_holder_segment_in_end_date_month_holder"),
        F.round(F.avg("holder_risk_score").over(w_product_holder_segment_by_pr), 2).alias("holder_risk_score_avg_3"),
        F.nth_value("end_date", 2).over(w_product_holder_segment_by_pr_2).alias("second_end_date"),
        F.sum("holder_risk_score").over(w_holder_segment_start_date_ye).alias("running_holder_risk_score"),
        F.round(F.cume_dist().over(w_holder_id_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.round(F.cume_dist().over(w_start_date_plus_7d_year_by_s), 3).alias("start_date_cume_dist"),
        F.ntile(4).over(w_product_holder_segment_by_pr_3).alias("premium_bucket_quartile"),
        F.size(F.collect_set("holder_segment").over(w_product_start_date_plus_7d_m)).alias("distinct_holder_segment_in_product_start_date_pl"),
        F.round(F.avg("holder_risk_score").over(w_holder_id_by_end_date_moving), 2).alias("holder_risk_score_avg_7"),
        F.lead("premium_bucket").over(w_holder_id_by_end_date).alias("next_premium_bucket"),
    )
)

summary = (
    ranked
    .filter(F.col("start_date").isNotNull())
    .groupBy(
        "holder_segment",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
    )
    .agg(
        F.sum(F.when(F.col("product") == "HEALTH", 1).otherwise(0)).alias("n_product_health"),
        F.max("running_holder_risk_score").alias("max_running_holder_risk_score"),
        F.min("end_date").alias("first_end_date"),
        F.round(F.avg("holder_risk_score_avg_7"), 2).alias("avg_holder_risk_score_avg_7"),
        F.min("start_date").alias("first_start_date"),
        F.round(F.sum("next_premium_bucket") / F.nullif(F.sum("distinct_holder_segment_in_end_date_month_holder"), F.lit(0)), 3).alias("next_premium_bucket_to_distinct_holder_segment_i"),
    )
)

sequenced = (
    summary
    .select(
        "holder_segment",
        "start_date_ym",
        "n_product_health",
        "max_running_holder_risk_score",
        "avg_holder_risk_score_avg_7",
        "first_start_date",
        F.max("avg_holder_risk_score_avg_7").over(w_first_end_date_month).alias("first_end_date_month_max_avg_holder_risk_score_a"),
        F.lag("n_product_health").over(w_all_by_max_running_holder_ri).alias("prev_n_product_health"),
        F.size(F.collect_set("holder_segment").over(w_first_end_date_month)).alias("distinct_holder_segment_in_first_end_date_month"),
    )
)

result = (
    sequenced
    .select(
        "holder_segment",
        "start_date_ym",
        "n_product_health",
        "max_running_holder_risk_score",
        "avg_holder_risk_score_avg_7",
        "first_start_date",
        "first_end_date_month_max_avg_holder_risk_score_a",
        "prev_n_product_health",
        "distinct_holder_segment_in_first_end_date_month",
        F.when(F.col("n_product_health") >= 7, "A").when(F.col("n_product_health") >= 2, "B").otherwise("C").alias("n_product_health_tier"),
        (F.floor(F.col("n_product_health") / 2) * 2).alias("n_product_health_bucket"),
    )
)
