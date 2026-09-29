from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_end_date_f = Window.partitionBy("product", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_holder_id = Window.partitionBy(F.month("end_date"), "holder_id")

recent = (
    df_policies
    .filter((F.col("holder_id") <= 4) & (~F.col("product").isin("TRAVEL", "AUTO")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.nth_value("premium_bucket", 2).over(w_product_status_by_end_date_f).alias("second_premium_bucket"),
        F.first("end_date", ignorenulls=True).over(w_all_by_end_date_running).alias("first_known_end_date"),
        F.size(F.collect_set("status").over(w_end_date_month_holder_id)).alias("distinct_status_in_end_date_month_holder_id"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_end_date_month_holder_id), 2).alias("premium_bucket_vs_end_date_month_holder_id_avg"),
    )
)

result = (
    with_trends
    .cube("status", "product", "distinct_status_in_end_date_month_holder_id")
    .agg(
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.sum("distinct_status_in_end_date_month_holder_id").alias("total_distinct_status_in_end_date_month_holder_i"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("distinct_status_in_end_date_month_holder_id").asc_nulls_last(),
    )
)
