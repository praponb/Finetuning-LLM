from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc_nulls_first())
w_status_end_date_month = Window.partitionBy("status", F.date_trunc("month", "end_date"))
w_status_product = Window.partitionBy("status", "product")
w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")
w_status_by_premium_moving_3 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-3, Window.currentRow)
w_end_date_month_by_start_date = Window.partitionBy(F.month("end_date")).orderBy("start_date", "id")
w_distinct_status_in_holder_id = Window.partitionBy("distinct_status_in_holder_id").orderBy(F.col("next_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_premium_dense_rank_b = Window.partitionBy("product", "premium_dense_rank_bucket").orderBy(F.col("prev_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_dense_rank_start_dat = Window.partitionBy("premium_dense_rank", "start_date_quartile")
w_distinct_status_in_holder_id_2 = Window.partitionBy("distinct_status_in_holder_id").orderBy(F.col("next_start_date").desc(), "id")
w_first_next_start_date_month = Window.partitionBy(F.date_trunc("month", "first_next_start_date"))
w_all_by_n_rows_ties = Window.orderBy(F.col("n_rows").desc())

scoped = (
    df_policies
    .filter((F.col("premium").between(2033.8, 3719.7)) & (F.col("product").isin("HOME", "HEALTH")))
)

metrics = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("status").over(w_holder_id)).alias("distinct_status_in_holder_id"),
        F.lead("start_date").over(w_all_by_end_date).alias("next_start_date"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.count(F.lit(1)).over(w_status_end_date_month).alias("rows_in_status_end_date_month"),
        F.count(F.lit(1)).over(w_status_product).alias("rows_in_status_product"),
        F.lag("start_date").over(w_all_by_premium).alias("prev_start_date"),
        F.size(F.collect_set("holder_id").over(w_status_end_date_month)).alias("distinct_holder_id_in_status_end_date_month"),
        F.round(F.avg("premium").over(w_status_end_date_month), 2).alias("status_end_date_month_avg_premium"),
        F.round(F.avg("premium").over(w_status_by_premium_moving_3), 2).alias("premium_avg_4"),
        F.ntile(4).over(w_end_date_month_by_start_date).alias("start_date_quartile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_product), F.lit(0)), 4).alias("premium_share_of_status_product"),
        F.round(F.avg("premium").over(w_status_product), 2).alias("status_product_avg_premium"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        (F.floor(F.col("premium_dense_rank") / 2) * 2).alias("premium_dense_rank_bucket"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.last("premium_avg_4", ignorenulls=True).over(w_distinct_status_in_holder_id).alias("last_known_premium_avg_4"),
        F.sum("premium_dense_rank").over(w_product_premium_dense_rank_b).alias("running_premium_dense_rank"),
        F.max("premium_avg_4").over(w_premium_dense_rank_start_dat).alias("premium_dense_rank_start_date_quartile_max_premi"),
        F.round(F.cume_dist().over(w_distinct_status_in_holder_id_2), 3).alias("next_start_date_cume_dist"),
        F.lead("start_date_quartile", 1, 0).over(w_distinct_status_in_holder_id_2).alias("next_start_date_quartile"),
    )
)

per_group = (
    ranked
    .groupBy("premium_dense_rank_bucket", "status")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.countDistinct("status").alias("distinct_status"),
        F.min("next_start_date").alias("first_next_start_date"),
    )
)

ranked_2 = (
    per_group
    .select(
        "premium_dense_rank_bucket",
        "status",
        "n_rows",
        "distinct_ids",
        "status_list",
        "distinct_status",
        "first_next_start_date",
        F.count(F.lit(1)).over(w_first_next_start_date_month).alias("rows_in_first_next_start_date_month"),
        F.round(F.cume_dist().over(w_all_by_n_rows_ties), 3).alias("n_rows_cume_dist"),
        F.round(F.avg("n_rows").over(w_first_next_start_date_month), 2).alias("first_next_start_date_month_avg_n_rows"),
    )
)

result = (
    ranked_2
    .select(
        "premium_dense_rank_bucket",
        "status",
        "n_rows",
        "distinct_ids",
        "status_list",
        "first_next_start_date",
        "rows_in_first_next_start_date_month",
        "n_rows_cume_dist",
        "first_next_start_date_month_avg_n_rows",
        (F.col("n_rows_cume_dist") - F.col("n_rows")).alias("n_rows_cume_dist_minus_n_rows"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy(
        F.col("first_next_start_date").desc(),
        F.col("status_slug").asc_nulls_last(),
    )
)
