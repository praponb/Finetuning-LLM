from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_start_date_month_product = Window.partitionBy(F.month("start_date"), "product")
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_prior_6 = Window.orderBy("premium", "id").rowsBetween(-6, -1)
w_product_status_by_premium_ti = Window.partitionBy("product", "status").orderBy("premium")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_premium_share_of_p = Window.partitionBy("status").orderBy(F.col("premium_share_of_product").desc_nulls_first())
w_start_date_month_status_by_p = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy(F.col("premium_share_of_product").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_status_by_p_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy(F.col("premium_share_of_product").desc(), "id")

filtered_policys = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

ranked = (
    filtered_policys
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status",
        "start_date_label",
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.max("premium").over(w_start_date_month_product).alias("start_date_month_product_max_premium"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.last("start_date").over(w_all_by_premium_full).alias("last_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.round(F.avg("premium").over(w_start_date_month_product), 2).alias("start_date_month_product_avg_premium"),
        F.round(F.avg("premium").over(w_all_by_premium_prior_6), 2).alias("premium_avg_6_prior"),
        F.size(F.collect_set("status").over(w_start_date_month_product)).alias("distinct_status_in_start_date_month_product"),
        F.dense_rank().over(w_product_status_by_premium_ti).alias("premium_dense_rank"),
        F.last("start_date", ignorenulls=True).over(w_all_by_premium_running).alias("last_known_start_date"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "start_date_label",
        "product_max_premium",
        "start_date_month_product_max_premium",
        "start_date_percent_rank",
        "last_start_date",
        "start_date_month_product_avg_premium",
        "premium_avg_6_prior",
        "distinct_status_in_start_date_month_product",
        "premium_dense_rank",
        "last_known_start_date",
        F.round(F.cume_dist().over(w_status_by_premium_share_of_p), 3).alias("premium_share_of_product_cume_dist"),
        F.last("premium_dense_rank").over(w_start_date_month_status_by_p).alias("last_premium_dense_rank"),
        F.lag("premium").over(w_start_date_month_status_by_p_2).alias("prev_premium"),
        F.max("premium_dense_rank").over(w_product).alias("product_max_premium_dense_rank"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
        F.round(F.col("premium_share_of_product") - F.avg("premium_share_of_product").over(w_product), 2).alias("premium_share_of_product_vs_product_avg"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "start_date_label",
        "product_max_premium",
        "start_date_month_product_max_premium",
        "last_start_date",
        "premium_avg_6_prior",
        "last_known_start_date",
        "premium_share_of_product_cume_dist",
        "last_premium_dense_rank",
        "prev_premium",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label_2"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy(F.col("last_known_start_date").asc_nulls_last())
)
