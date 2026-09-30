from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium_running = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_product_by_premium_moving_6 = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_all_by_last_known_start_date = Window.orderBy("last_known_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

with_windows = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "product_len",
        F.last("start_date", ignorenulls=True).over(w_product_by_premium_running).alias("last_known_start_date"),
        F.ntile(4).over(w_product_by_premium).alias("premium_quartile"),
        F.lag("start_date", 2).over(w_product_by_premium).alias("prev2_start_date"),
        F.row_number().over(w_product_by_premium).alias("rn"),
        F.lag("premium").over(w_product_by_premium).alias("prev_premium"),
        F.round(F.avg("product_len").over(w_product_by_premium_moving_6), 2).alias("product_len_avg_7"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "holder_id",
        "start_date",
        "product_len",
        "premium_quartile",
        "rn",
        "product_len_avg_7",
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.first("end_date").over(w_all_by_last_known_start_date).alias("first_end_date"),
        F.last("prev_premium").over(w_all_by_last_known_start_date).alias("last_prev_premium"),
        F.size(F.collect_set("holder_id").over(w_end_date_month)).alias("distinct_holder_id_in_end_date_month"),
    )
)

top_rows = (
    ranked
    .filter((F.col("rn") <= 5) & (F.col("start_date") < F.lit("2025-03-24").cast("date")))
    .drop("rn")
)

result = (
    top_rows
    .groupingSets(
        [["product_len", "holder_id"], ["product_len"]],
        "product_len", "holder_id",
    )
    .agg(
        F.round(F.sum("distinct_holder_id_in_end_date_month") / F.nullif(F.sum("product_len"), F.lit(0)), 3).alias("distinct_holder_id_in_end_date_month_to_product_"),
        F.round(F.sum("last_prev_premium"), 2).alias("total_last_prev_premium"),
        F.datediff(F.max("first_end_date"), F.min("first_end_date")).alias("first_end_date_span_days"),
    )
    .orderBy(
        F.col("product_len").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
