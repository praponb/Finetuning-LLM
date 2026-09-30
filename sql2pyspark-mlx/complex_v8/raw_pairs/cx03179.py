from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_end_date_ties = Window.partitionBy("product").orderBy(F.col("end_date").asc_nulls_last())
w_status_by_premium_full = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product = Window.partitionBy("product")
w_all_by_premium_prior_1 = Window.orderBy("premium", "id").rowsBetween(-1, -1)
w_status_by_premium_moving_2 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-2, Window.currentRow)
w_product_by_start_date_runnin = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_premium_moving_1 = Window.orderBy("premium", "id").rowsBetween(-1, Window.currentRow)
w_all_by_premium_ties = Window.orderBy("premium")
w_status_running_premium_tier = Window.partitionBy("status", "running_premium_tier").orderBy(F.col("n_status_is_not_null").asc_nulls_last(), "status", "running_premium_tier", "end_date_ym")
w_all_by_median_product_avg_pr = Window.orderBy("median_product_avg_premium", "status", "running_premium_tier", "end_date_ym").rowsBetween(-3, Window.currentRow)
w_n_product_travel_n_status_is = Window.partitionBy("n_product_travel", "n_status_is_not_null")
w_all_by_median_product_avg_pr_2 = Window.orderBy("median_product_avg_premium", "status", "running_premium_tier", "end_date_ym")

prepared = df_policies.filter((F.col("holder_id").isNotNull()) & (F.col("status") == "EXPIRED"))

windowed = (
    prepared
    .select(
        "*",
        F.round(F.cume_dist().over(w_product_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.nth_value("premium", 2).over(w_status_by_premium_full).alias("second_premium"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.max("premium").over(w_all_by_premium_prior_1).alias("premium_max_1_prior"),
        F.round(F.avg("premium").over(w_status_by_premium_moving_2), 2).alias("premium_avg_3"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.sum("premium").over(w_product_by_start_date_runnin).alias("running_premium"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist_2"),
        F.round(F.avg("premium").over(w_all_by_premium_moving_1), 2).alias("premium_avg_2"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
    )
)

scored = (
    windowed
    .filter(F.col("premium") > 1132.1)
    .withColumn("status_ref", F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")))
)

per_group = (
    scored
    .groupBy(
        "status",
        F.when(F.col("running_premium") >= 32570.0, "HIGH").when(F.col("running_premium") >= 15140.0, "MEDIUM").otherwise("LOW").alias("running_premium_tier"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.round(F.sum("premium_avg_2") / F.nullif(F.sum("end_date_cume_dist"), F.lit(0)), 3).alias("premium_avg_2_to_end_date_cume_dist_ratio"),
        F.sum(F.when(F.col("status").isNotNull(), 1).otherwise(0)).alias("n_status_is_not_null"),
        F.sum(F.when(F.col("product") == "TRAVEL", 1).otherwise(0)).alias("n_product_travel"),
        F.percentile_approx("product_avg_premium", 0.5).alias("median_product_avg_premium"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)

sequenced = (
    per_group
    .select(
        "status",
        "running_premium_tier",
        "end_date_ym",
        "premium_avg_2_to_end_date_cume_dist_ratio",
        "n_status_is_not_null",
        "n_product_travel",
        "product_list",
        F.round(F.cume_dist().over(w_status_running_premium_tier), 3).alias("n_status_is_not_null_cume_dist"),
        F.sum("median_product_avg_premium").over(w_all_by_median_product_avg_pr).alias("median_product_avg_premium_sum_4"),
        F.round(F.col("n_product_travel") / F.nullif(F.sum("n_product_travel").over(w_n_product_travel_n_status_is), F.lit(0)), 4).alias("n_product_travel_share_of_n_product_travel_n_sta"),
        F.dense_rank().over(w_all_by_median_product_avg_pr_2).alias("median_product_avg_premium_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "status",
        "running_premium_tier",
        "end_date_ym",
        "premium_avg_2_to_end_date_cume_dist_ratio",
        "n_status_is_not_null",
        "n_product_travel",
        "n_status_is_not_null_cume_dist",
        "median_product_avg_premium_sum_4",
        "n_product_travel_share_of_n_product_travel_n_sta",
        "median_product_avg_premium_dense_rank",
        F.initcap(F.trim(F.col("running_premium_tier"))).alias("running_premium_tier_clean"),
    )
    .orderBy(F.col("running_premium_tier_clean").desc())
)
