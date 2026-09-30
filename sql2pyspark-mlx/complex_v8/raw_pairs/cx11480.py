from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_premium_prior_6 = Window.partitionBy("status").orderBy("premium", "id").rowsBetween(-6, -1)
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy("premium", "id")
w_start_date_year_product = Window.partitionBy(F.year("start_date"), "product")
w_holder_id = Window.partitionBy("holder_id")
w_start_date_month_by_running = Window.partitionBy(F.month("start_date")).orderBy("running_premium")
w_holder_id_distinct_status_in = Window.partitionBy("holder_id", "distinct_status_in_product_holder_id").orderBy("premium_share_of_holder_id", "id")
w_start_date_month_by_running_2 = Window.partitionBy(F.month("start_date")).orderBy("running_premium", "id")

recent = (
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

with_windows = (
    recent
    .select(
        "*",
        F.size(F.collect_set("status").over(w_product_holder_id)).alias("distinct_status_in_product_holder_id"),
        F.sum("premium").over(w_holder_id_by_start_date_runn).alias("running_premium"),
        F.first("premium", ignorenulls=True).over(w_product_holder_id_by_premium).alias("first_known_premium"),
        F.sum("premium").over(w_status_by_premium_prior_6).alias("premium_sum_6_prior"),
        F.lag("premium", 2).over(w_holder_id_status_by_premium).alias("prev2_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_year_product), F.lit(0)), 4).alias("premium_share_of_start_date_year_product"),
        F.row_number().over(w_holder_id_status_by_premium).alias("seq_no"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
    )
)

top_rows = with_windows.filter(F.col("seq_no") == 1)

with_windows_2 = (
    top_rows
    .select(
        "id",
        "product",
        "start_date",
        "status",
        "distinct_status_in_product_holder_id",
        "running_premium",
        "first_known_premium",
        "prev2_premium",
        "premium_share_of_start_date_year_product",
        "seq_no",
        "premium_share_of_holder_id",
        F.round(F.percent_rank().over(w_start_date_month_by_running), 3).alias("running_premium_percent_rank"),
        F.ntile(4).over(w_holder_id_distinct_status_in).alias("premium_share_of_holder_id_quartile"),
        F.rank().over(w_start_date_month_by_running_2).alias("running_premium_rank"),
    )
)

rollup_base = (
    with_windows_2
    .groupBy(
        "running_premium_rank",
        "seq_no",
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.round(F.avg("running_premium_rank"), 2).alias("avg_running_premium_rank"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("n_status_cancelled"),
        F.min("distinct_status_in_product_holder_id").alias("min_distinct_status_in_product_holder_id"),
        F.round(F.sum("first_known_premium"), 2).alias("total_first_known_premium"),
        F.count("*").alias("cnt"),
    )
)

result = (
    rollup_base
    .filter(F.hour(F.col("start_date_month")).between(10, 12))
    .select(
        "running_premium_rank",
        "seq_no",
        "start_date_month",
        "min_distinct_status_in_product_holder_id",
        "total_first_known_premium",
        "cnt",
        F.round(F.col("cnt") / F.nullif(F.col("avg_running_premium_rank"), F.lit(0)), 3).alias("cnt_per_avg_running_premium_rank"),
        F.round(F.col("cnt") / F.nullif(F.col("running_premium_rank"), F.lit(0)), 3).alias("cnt_per_running_premium_rank"),
    )
    .orderBy(F.col("total_first_known_premium").desc())
)
