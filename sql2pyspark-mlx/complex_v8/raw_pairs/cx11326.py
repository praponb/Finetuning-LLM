from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc_nulls_first(), "id")
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id")
w_product_seq_no_by_start_date = Window.partitionBy("product", "seq_no").orderBy("start_date", "id")
w_seq_no_start_date_rank = Window.partitionBy("seq_no", "start_date_rank")
w_product_seq_no_by_start_date_2 = Window.partitionBy("product", "seq_no").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_product_status_by_distinct_h = Window.partitionBy("product", "status").orderBy(F.col("distinct_holder_id_in_product_to_start_date_perc").desc(), "product", "premium_quintile", "status")
w_product_status_by_distinct_h_2 = Window.partitionBy("product", "status").orderBy(F.col("distinct_holder_id_in_product_to_start_date_perc").desc(), "product", "premium_quintile", "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

metrics = (
    valid_policys
    .select(
        "*",
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.row_number().over(w_all_by_end_date).alias("seq_no"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.rank().over(w_product_by_start_date).alias("start_date_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_product_seq_no_by_start_date), 3).alias("start_date_percent_rank"),
        F.round(F.col("premium_share_of_product") - F.avg("premium_share_of_product").over(w_seq_no_start_date_rank), 2).alias("premium_share_of_product_vs_seq_no_start_date_ra"),
        F.nth_value("rows_in_product", 2).over(w_product_seq_no_by_start_date_2).alias("second_rows_in_product"),
        F.size(F.collect_set("status").over(w_seq_no_start_date_rank)).alias("distinct_status_in_seq_no_start_date_rank"),
        F.max("rows_in_product").over(w_seq_no_start_date_rank).alias("seq_no_start_date_rank_max_rows_in_product"),
        F.ntile(5).over(w_all_by_premium).alias("premium_quintile"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
    )
)

summary = (
    with_windows
    .groupBy("product", "premium_quintile", "status")
    .agg(
        F.round(F.sum("distinct_holder_id_in_product") / F.nullif(F.sum("start_date_percent_rank"), F.lit(0)), 3).alias("distinct_holder_id_in_product_to_start_date_perc"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.sum("distinct_status_in_seq_no_start_date_rank").alias("sum_distinct_status_in_seq_no_start_date_rank_ch"),
    )
    .filter(F.col("sum_distinct_status_in_seq_no_start_date_rank_ch") > 4)
    .drop("sum_distinct_status_in_seq_no_start_date_rank_ch")
)

metrics_2 = (
    summary
    .withColumn("premium_quintile_change", F.col("premium_quintile") - F.lag("premium_quintile").over(w_product_status_by_distinct_h))
    .withColumn("last_known_premium_quintile", F.last("premium_quintile", ignorenulls=True).over(w_product_status_by_distinct_h_2))
)

result = (
    metrics_2
    .select(
        "product",
        "premium_quintile",
        "status",
        "distinct_holder_id_in_product_to_start_date_perc",
        "premium_quintile_change",
        "last_known_premium_quintile",
    )
)
