from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_avg_prev_premium_prio = Window.orderBy(F.col("avg_prev_premium").desc(), "product", "distinct_product_in_status").rowsBetween(-2, -1)
w_distinct_product_in_status_p = Window.partitionBy("distinct_product_in_status", "product")
w_all_by_avg_prev_premium_runn = Window.orderBy(F.col("avg_prev_premium").desc(), "product", "distinct_product_in_status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_avg_prev_premium_ties = Window.orderBy(F.col("avg_prev_premium").desc())

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

ranked = (
    prepared
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
        F.lag("premium").over(w_all_by_start_date).alias("prev_premium"),
    )
)

rollup_base = (
    ranked
    .groupBy("product", "distinct_product_in_status")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.round(F.avg("prev_premium"), 2).alias("avg_prev_premium"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)

windowed = (
    rollup_base
    .select(
        "*",
        F.max("distinct_product_in_status").over(w_all_by_avg_prev_premium_prio).alias("distinct_product_in_status_max_2_prior"),
        F.sum("distinct_product_in_status").over(w_all_by_avg_prev_premium_runn).alias("running_distinct_product_in_status"),
        F.rank().over(w_all_by_avg_prev_premium_ties).alias("avg_prev_premium_rank"),
        F.min("distinct_product_in_status").over(w_distinct_product_in_status_p).alias("distinct_product_in_status_product_min_distinct_"),
        F.round(F.col("distinct_product_in_status") - F.avg("distinct_product_in_status").over(w_distinct_product_in_status_p), 2).alias("distinct_product_in_status_vs_distinct_product_i"),
    )
)

derived = (
    windowed
    .filter(F.size(F.col("product_list")) >= 1)
    .select(
        "*",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.round(F.col("avg_prev_premium") * 1.07, 2).alias("avg_prev_premium_adj"),
        F.round(F.col("avg_prev_premium") / F.nullif(F.col("avg_prev_premium_rank"), F.lit(0)), 3).alias("avg_prev_premium_per_avg_prev_premium_rank"),
    )
)

qualified = (
    derived
    .crossJoin(derived.agg(F.avg("avg_premium").alias("overall_avg_avg_premium")))
    .filter(F.col("avg_premium") > F.col("overall_avg_avg_premium"))
    .drop("overall_avg_avg_premium")
)

with_activity = (
    qualified
    .crossJoin(qualified.agg(F.min("product_len").alias("overall_min_product_len")))
    .filter(F.col("product_len") > F.col("overall_min_product_len"))
    .drop("overall_min_product_len")
)

result = (
    with_activity
    .select(
        "product",
        "distinct_product_in_status",
        "avg_premium",
        "avg_prev_premium",
        "product_list",
        "distinct_product_in_status_max_2_prior",
        "running_distinct_product_in_status",
        "distinct_product_in_status_product_min_distinct_",
        "distinct_product_in_status_vs_distinct_product_i",
        "product_len",
        "avg_prev_premium_per_avg_prev_premium_rank",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
    .orderBy(F.col("avg_prev_premium_per_avg_prev_premium_rank").desc())
)
