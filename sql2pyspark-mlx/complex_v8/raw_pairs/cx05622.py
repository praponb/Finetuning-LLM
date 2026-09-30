from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_month = Window.partitionBy("holder_id", F.month("start_date"))
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_end_dat = Window.partitionBy("holder_id", "product").orderBy("end_date", "id")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month_product = Window.partitionBy(F.month("start_date"), "product")
w_product_holder_id_by_premium_2 = Window.partitionBy("product", "holder_id").orderBy("premium", "id").rowsBetween(-3, -1)
w_status_product_by_start_date = Window.partitionBy("status", "product").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)

cleaned_policys = df_policies.filter((F.col("status").isNotNull()) & (F.col("premium") > 1051.9))

with_windows = (
    cleaned_policys
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_start_date_month), F.lit(0)), 4).alias("premium_share_of_holder_id_start_date_month"),
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.nth_value("end_date", 2).over(w_holder_id_by_start_date_full).alias("second_end_date"),
        F.count(F.lit(1)).over(w_holder_id_start_date_month).alias("rows_in_holder_id_start_date_month"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_product_by_end_dat)).alias("days_since_prev_start_date"),
        F.sum("premium").over(w_product_holder_id_by_premium).alias("running_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month_product), 2).alias("premium_vs_start_date_month_product_avg"),
        F.round(F.avg("premium").over(w_product_holder_id_by_premium_2), 2).alias("premium_avg_3_prior"),
        F.round(F.avg("premium").over(w_status_product_by_start_date), 2).alias("premium_avg_4"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

stats = (
    derived
    .groupBy(F.year("start_date").alias("start_date_year"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.percentile_approx("premium_vs_start_date_month_product_avg", 0.5).alias("median_premium_vs_start_date_month_product_avg"),
        F.round(F.sum("premium_avg_4"), 2).alias("total_premium_avg_4"),
        F.min("running_premium").alias("min_running_premium"),
    )
)

result = (
    stats
    .select(
        "start_date_year",
        "distinct_holder_ids",
        "median_premium_vs_start_date_month_product_avg",
    )
    .orderBy(
        F.col("distinct_holder_ids").desc(),
        F.col("median_premium_vs_start_date_month_product_avg").desc(),
        "start_date_year",
    )
    .limit(5)
)
