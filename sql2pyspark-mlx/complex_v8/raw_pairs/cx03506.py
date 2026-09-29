from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc())
w_product = Window.partitionBy("product")
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")
w_product_holder_id_by_premium_2 = Window.partitionBy("product", "holder_id").orderBy("premium_filled", "id")
w_seq_no_holder_id_by_days_sin = Window.partitionBy("seq_no", "holder_id").orderBy("days_since_prev_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_end_date_month_by_premium_pe = Window.partitionBy(F.month("end_date")).orderBy(F.col("premium_percent_rank_2").desc(), "id")
w_start_date_month_by_running = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("running_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no = Window.partitionBy("seq_no")

cleaned = (
    df_policies
    .filter((F.col("holder_id") >= 4) & (F.col("end_date") >= F.lit("2026-02-16").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_all_by_premium).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_start_date)).alias("days_since_prev_end_date"),
        F.sum("premium").over(w_product_holder_id_by_premium).alias("running_premium"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_start_date)).alias("days_since_prev_start_date"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank_2"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
        F.round(F.avg("premium_filled").over(w_start_date_month_holder_id), 2).alias("start_date_month_holder_id_avg_premium_filled"),
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.rank().over(w_product_holder_id_by_premium_2).alias("premium_filled_rank"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "premium_filled",
        "seq_no",
        "days_since_prev_end_date",
        "running_premium",
        "premium_percent_rank_2",
        "start_date_percent_rank",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

metrics_2 = (
    annotated
    .select(
        "*",
        F.first("product_len", ignorenulls=True).over(w_seq_no_holder_id_by_days_sin).alias("first_known_product_len"),
        F.first("start_date_percent_rank", ignorenulls=True).over(w_seq_no_holder_id_by_days_sin).alias("first_known_start_date_percent_rank"),
        F.round(F.col("days_since_prev_end_date") / F.nullif(F.sum("days_since_prev_end_date").over(w_start_date_month), F.lit(0)), 4).alias("days_since_prev_end_date_share_of_start_date_mon"),
        F.lag("premium_filled", 1, 0).over(w_end_date_month_by_premium_pe).alias("prev_premium_filled"),
        F.first("seq_no", ignorenulls=True).over(w_start_date_month_by_running).alias("first_known_seq_no"),
        F.sum("days_since_prev_end_date").over(w_start_date_month).alias("start_date_month_sum_days_since_prev_end_date"),
        F.max("seq_no").over(w_seq_no).alias("seq_no_max_seq_no"),
    )
)

summary = (
    metrics_2
    .filter(F.col("days_since_prev_end_date") >= 50)
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(F.array_sort(F.collect_set("product")).alias("product_list"))
)

result = (
    summary
    .filter(F.size(F.col("product_list")) >= 1)
    .select("end_date_month", "product_list")
    .orderBy(F.col("end_date_month").asc_nulls_last())
    .limit(20)
)
