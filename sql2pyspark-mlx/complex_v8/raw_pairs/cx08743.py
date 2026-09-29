from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc_nulls_first(), "id")
w_product = Window.partitionBy("product")
w_product_by_start_date_runnin = Window.partitionBy("product").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_product_by_start_date_ties = Window.partitionBy("product").orderBy(F.col("start_date").desc_nulls_first())
w_holder_id_product_by_premium_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_premium_3 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_status_product = Window.partitionBy("status", "product")
w_status_product_by_days_since = Window.partitionBy("status", "product").orderBy("days_since_prev_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_product_sum_premiu = Window.partitionBy("status").orderBy("product_sum_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_product_sum_premiu_2 = Window.partitionBy("status").orderBy("product_sum_premium", "id")
w_status_product_by_days_since_2 = Window.partitionBy("status", "product").orderBy("days_since_prev_start_date", "id")

filtered = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-01-20").cast("date"))
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
    filtered
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        F.lead("premium", 3).over(w_holder_id_product_by_premium).alias("next3_premium"),
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.first("start_date", ignorenulls=True).over(w_product_by_start_date_runnin).alias("first_known_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_product_by_premium)).alias("days_since_prev_start_date"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.row_number().over(w_holder_id_by_premium).alias("seq_no"),
        F.round(F.percent_rank().over(w_product_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.nth_value("start_date", 2).over(w_holder_id_product_by_premium_2).alias("second_start_date"),
        F.round(F.avg("premium").over(w_holder_id_product_by_premium_3), 2).alias("premium_avg_3"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "product",
        "status",
        "product_sum_premium",
        "days_since_prev_start_date",
        "product_max_premium",
        "rows_in_product",
        "product_avg_premium",
        "start_date_percent_rank",
        "second_start_date",
        "premium_avg_3",
        F.least(F.col("product_avg_premium"), F.col("seq_no")).alias("least_product_avg_premium_seq_no"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.round(F.avg("premium_avg_3").over(w_status_product), 2).alias("status_product_avg_premium_avg_3"),
        F.count(F.lit(1)).over(w_status_product).alias("rows_in_status_product"),
        F.nth_value("second_start_date", 2).over(w_status_product_by_days_since).alias("second_second_start_date"),
        F.first("rows_in_product", ignorenulls=True).over(w_status_by_product_sum_premiu).alias("first_known_rows_in_product"),
        F.ntile(10).over(w_status_by_product_sum_premiu_2).alias("product_sum_premium_decile"),
        F.lead("premium_avg_3").over(w_status_product_by_days_since_2).alias("next_premium_avg_3"),
        F.sum("days_since_prev_start_date").over(w_status_product).alias("status_product_sum_days_since_prev_start_date"),
    )
)

result = (
    metrics
    .filter(F.col("least_product_avg_premium_seq_no") >= 2000.6)
    .select(
        "id",
        "product",
        "status",
        "product_sum_premium",
        "days_since_prev_start_date",
        "product_max_premium",
        "product_avg_premium",
        "start_date_percent_rank",
        "premium_avg_3",
        "least_product_avg_premium_seq_no",
        "status_clean",
        "product_prefix",
        "status_product_avg_premium_avg_3",
        "rows_in_status_product",
        "second_second_start_date",
        "first_known_rows_in_product",
        "product_sum_premium_decile",
        "next_premium_avg_3",
        "status_product_sum_days_since_prev_start_date",
        (F.floor(F.col("rows_in_status_product") / 5) * 5).alias("rows_in_status_product_bucket"),
        F.round(F.col("product_avg_premium") * 1.1, 2).alias("product_avg_premium_adj"),
    )
    .orderBy(
        F.col("product_avg_premium_adj").asc(),
        F.col("first_known_rows_in_product").asc_nulls_last(),
    )
)
