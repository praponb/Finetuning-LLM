from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_status_by_sta = Window.partitionBy(F.date_trunc("month", "end_date"), "status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_holder_id_by = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("start_date").desc_nulls_first())
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")
w_end_date_month_holder_id_by_2 = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_status = Window.partitionBy("status")
w_status_start_date_year_by_pr = Window.partitionBy("status", F.year("start_date")).orderBy(F.col("premium").desc(), "id")
w_premium_quartile_by_premium = Window.partitionBy("premium_quartile").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_premium_quartile_by_premium_2 = Window.partitionBy("premium_quartile").orderBy(F.col("premium").desc(), "id")
w_premium_quartile_by_premium_3 = Window.partitionBy("premium_quartile").orderBy(F.col("premium").desc())
w_premium_quartile_by_prev_pre = Window.partitionBy("premium_quartile").orderBy(F.col("prev_premium").desc(), "id")

recent = (
    df_policies
    .filter(F.col("premium") >= 1042.5)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.max("premium").over(w_end_date_month_status_by_sta).alias("max_premium_so_far"),
        F.round(F.percent_rank().over(w_end_date_month_holder_id_by), 3).alias("start_date_percent_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month_holder_id), 2).alias("premium_vs_start_date_month_holder_id_avg"),
        F.lag("premium").over(w_end_date_month_holder_id_by_2).alias("prev_premium"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.ntile(4).over(w_status_start_date_year_by_pr).alias("premium_quartile"),
        F.min("premium").over(w_start_date_month_holder_id).alias("start_date_month_holder_id_min_premium"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "end_date",
        "max_premium_so_far",
        "start_date_percent_rank",
        "premium_vs_start_date_month_holder_id_avg",
        "prev_premium",
        "status_min_premium",
        "premium_quartile",
        "start_date_month_holder_id_min_premium",
        F.max("prev_premium").over(w_premium_quartile_by_premium).alias("prev_premium_max_2"),
        F.row_number().over(w_premium_quartile_by_premium_2).alias("seq_no"),
        F.dense_rank().over(w_premium_quartile_by_premium_3).alias("premium_dense_rank"),
        F.rank().over(w_premium_quartile_by_prev_pre).alias("prev_premium_rank"),
    )
)

rollup_base = (
    ranked
    .filter(F.col("status_min_premium").between(15140.0, 37550.0))
    .groupBy("premium_dense_rank")
    .agg(
        F.round(F.stddev("premium_quartile"), 2).alias("stddev_premium_quartile"),
        F.round(F.stddev("prev_premium"), 2).alias("stddev_prev_premium"),
        F.avg("max_premium_so_far").alias("avg_max_premium_so_far_check"),
    )
    .filter(F.col("avg_max_premium_so_far_check") > 10160.0)
    .drop("avg_max_premium_so_far_check")
)

result = (
    rollup_base
    .filter(F.col("stddev_premium_quartile") > 1.0)
    .select("premium_dense_rank", "stddev_premium_quartile", "stddev_prev_premium")
)
