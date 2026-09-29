from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_start_date = Window.orderBy("days_since_start_date", "id")
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_product_by_premium_full = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_start_date_2 = Window.orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_product_by_rows_in_end_date = Window.partitionBy("product").orderBy("rows_in_end_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_holder_id_in_end_da = Window.partitionBy("distinct_holder_id_in_end_date_month", "status").orderBy(F.col("days_since_start_date_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_rows_in_end_date_2 = Window.partitionBy("product").orderBy("rows_in_end_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_by_days_since = Window.partitionBy(F.month("end_date")).orderBy(F.col("days_since_start_date_percent_rank").desc())

valid = (
    df_policies
    .filter(F.col("premium") >= 2289.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_days_since_start_date), 3).alias("days_since_start_date_percent_rank"),
        F.size(F.collect_set("holder_id").over(w_end_date_month)).alias("distinct_holder_id_in_end_date_month"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.nth_value("premium", 2).over(w_product_by_premium_full).alias("second_premium"),
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_end_date_month), 2).alias("days_since_start_date_vs_end_date_month_avg"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month), 2).alias("premium_vs_end_date_month_avg"),
        (F.col("days_since_start_date") - F.lag("days_since_start_date").over(w_all_by_days_since_start_date)).alias("days_since_start_date_change"),
        F.nth_value("premium", 2).over(w_all_by_days_since_start_date_2).alias("second_premium_2"),
    )
)

enriched = (
    ranked
    .filter(F.col("days_since_start_date_percent_rank") > 0.3)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("status").over(w_holder_id)).alias("distinct_status_in_holder_id"),
        F.max("rows_in_end_date_month").over(w_product_by_rows_in_end_date).alias("max_rows_in_end_date_month_so_far"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.max("premium").over(w_distinct_holder_id_in_end_da).alias("max_premium_so_far"),
        F.nth_value("premium", 2).over(w_product_by_rows_in_end_date_2).alias("second_premium_3"),
        F.rank().over(w_end_date_month_by_days_since).alias("days_since_start_date_percent_rank_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "end_date_label",
        "days_since_start_date_percent_rank",
        "rows_in_end_date_month",
        "second_premium",
        "days_since_start_date_vs_end_date_month_avg",
        "premium_vs_end_date_month_avg",
        "days_since_start_date_change",
        "second_premium_2",
        "days_since_end_date",
        "start_date_month",
        "distinct_status_in_holder_id",
        "max_rows_in_end_date_month_so_far",
        "rows_in_holder_id",
        "max_premium_so_far",
        "second_premium_3",
        "days_since_start_date_percent_rank_rank",
    )
    .orderBy(F.col("premium").desc_nulls_first(), "rows_in_end_date_month")
)
