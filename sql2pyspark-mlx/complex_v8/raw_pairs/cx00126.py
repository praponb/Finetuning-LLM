from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc_nulls_first(), "id")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_premium_moving_3 = Window.partitionBy("product").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_end_date_year_by_end_date_fu = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_in_product_b = Window.partitionBy("distinct_status_in_product").orderBy("last_premium_2", "id")
w_start_date_month_product = Window.partitionBy(F.date_trunc("month", "start_date"), "product")
w_all_by_end_date = Window.orderBy("end_date", "id")

scoped_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") != "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

metrics = (
    scoped_policys
    .select(
        "*",
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.ntile(3).over(w_product_by_premium).alias("premium_tercile"),
        F.last("premium").over(w_all_by_start_date_full).alias("last_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
        F.round(F.avg("premium").over(w_product_by_premium_moving_3), 2).alias("premium_avg_4"),
        F.last("premium").over(w_end_date_year_by_end_date_fu).alias("last_premium_2"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_distinct_status_in_product_b)).alias("days_since_prev_start_date"),
        F.round(F.col("product_min_premium") - F.avg("product_min_premium").over(w_start_date_month_product), 2).alias("product_min_premium_vs_start_date_month_product_"),
        F.ntile(10).over(w_all_by_end_date).alias("end_date_decile"),
        F.size(F.collect_set("holder_id").over(w_start_date_month_product)).alias("distinct_holder_id_in_start_date_month_product"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month_product), 2).alias("premium_vs_start_date_month_product_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status",
        "product_min_premium",
        "rows_in_end_date_month",
        "product_avg_premium",
        "premium_share_of_product",
        "distinct_status_in_product",
        "premium_avg_4",
        "last_premium_2",
        "days_since_prev_start_date",
        "end_date_decile",
        "distinct_holder_id_in_start_date_month_product",
        "premium_vs_start_date_month_product_avg",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        (F.col("premium_share_of_product") + F.col("last_premium_2")).alias("premium_share_of_product_plus_last_premium_2"),
    )
)
