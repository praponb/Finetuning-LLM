from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_stddev_unit_cos = Window.partitionBy("is_active").orderBy(F.col("stddev_unit_cost_bucket").desc(), "is_active").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_by_pct_is_active_t = Window.partitionBy("is_active").orderBy("pct_is_active_true", "is_active")
w_is_active_by_pct_is_active_t_2 = Window.partitionBy("is_active").orderBy("pct_is_active_true", "is_active").rowsBetween(-2, Window.currentRow)
w_is_active = Window.partitionBy("is_active")
w_all_by_stddev_unit_cost_buck = Window.orderBy(F.col("stddev_unit_cost_bucket").desc(), "is_active").rowsBetween(-1, -1)
w_all_by_stddev_unit_cost_buck_2 = Window.orderBy(F.col("stddev_unit_cost_bucket").desc(), "is_active").rowsBetween(-1, Window.currentRow)
w_all_by_stddev_unit_cost_buck_3 = Window.orderBy(F.col("stddev_unit_cost_bucket").desc(), "is_active")

recent = (
    df_skus
    .filter((F.col("sku_code").isNotNull()) | (F.col("weight_kg").between(1.8, 20.8)))
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
    )
)

summary = (
    recent
    .groupBy("is_active")
    .agg(
        F.round(F.stddev("unit_cost_bucket"), 2).alias("stddev_unit_cost_bucket"),
        F.round(100 * F.count(F.when(F.col("is_active"), 1)) / F.count("*"), 1).alias("pct_is_active_true"),
        F.max("weight_kg").alias("max_weight_kg_check"),
    )
    .filter(F.col("max_weight_kg_check") > 4.7)
    .drop("max_weight_kg_check")
)

without_matches = (
    summary
    .crossJoin(summary.agg(F.avg("stddev_unit_cost_bucket").alias("overall_avg_stddev_unit_cost_bucket")))
    .filter(F.col("stddev_unit_cost_bucket") > F.col("overall_avg_stddev_unit_cost_bucket"))
    .drop("overall_avg_stddev_unit_cost_bucket")
)

ranked = (
    without_matches
    .select(
        "is_active",
        "stddev_unit_cost_bucket",
        F.min("pct_is_active_true").over(w_is_active_by_stddev_unit_cos).alias("min_pct_is_active_true_so_far"),
        F.lead("pct_is_active_true", 1, 0).over(w_is_active_by_pct_is_active_t).alias("next_pct_is_active_true"),
        F.sum("stddev_unit_cost_bucket").over(w_is_active_by_pct_is_active_t_2).alias("stddev_unit_cost_bucket_sum_3"),
        F.round(F.col("pct_is_active_true") / F.nullif(F.sum("pct_is_active_true").over(w_is_active), F.lit(0)), 4).alias("pct_is_active_true_share_of_is_active"),
    )
)

with_activity = (
    ranked
    .crossJoin(ranked.agg(F.min("pct_is_active_true_share_of_is_active").alias("overall_min_pct_is_active_true_share_of_is_activ")))
    .filter(F.col("pct_is_active_true_share_of_is_active") > F.col("overall_min_pct_is_active_true_share_of_is_activ"))
    .drop("overall_min_pct_is_active_true_share_of_is_activ")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.max("next_pct_is_active_true").alias("overall_max_next_pct_is_active_true")))
    .filter(F.col("next_pct_is_active_true") < F.col("overall_max_next_pct_is_active_true"))
    .drop("overall_max_next_pct_is_active_true")
)

with_trends = (
    with_activity_2
    .select(
        "is_active",
        "min_pct_is_active_true_so_far",
        "next_pct_is_active_true",
        "stddev_unit_cost_bucket_sum_3",
        F.sum("next_pct_is_active_true").over(w_all_by_stddev_unit_cost_buck).alias("next_pct_is_active_true_sum_1_prior"),
        F.round(F.avg("next_pct_is_active_true").over(w_all_by_stddev_unit_cost_buck_2), 2).alias("next_pct_is_active_true_avg_2"),
        F.round(F.col("next_pct_is_active_true") - F.avg("next_pct_is_active_true").over(w_is_active), 2).alias("next_pct_is_active_true_vs_is_active_avg"),
        F.row_number().over(w_all_by_stddev_unit_cost_buck_3).alias("seq_no"),
        F.lag("stddev_unit_cost_bucket").over(w_all_by_stddev_unit_cost_buck_3).alias("prev_stddev_unit_cost_bucket"),
        F.lag("next_pct_is_active_true", 3, 0).over(w_all_by_stddev_unit_cost_buck_3).alias("prev3_next_pct_is_active_true"),
    )
)

result = (
    with_trends
    .select(
        "is_active",
        "min_pct_is_active_true_so_far",
        "next_pct_is_active_true_sum_1_prior",
        "next_pct_is_active_true_vs_is_active_avg",
        "seq_no",
        "prev_stddev_unit_cost_bucket",
        "prev3_next_pct_is_active_true",
    )
    .orderBy(F.col("seq_no").asc(), "is_active")
    .limit(10)
)
