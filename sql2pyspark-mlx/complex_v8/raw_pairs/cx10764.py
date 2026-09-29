from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_product = Window.partitionBy(F.date_trunc("month", "end_date"), "product")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product_by_start_d = Window.partitionBy("holder_id", "product").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_product_by_end_date = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id")
w_product_by_end_date_full = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_status = Window.partitionBy("product", "status")
w_premium_quintile_first_start = Window.partitionBy("premium_quintile", F.year("first_start_date_month")).orderBy("premium_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_quintile_first_start_2 = Window.partitionBy("premium_quintile", F.year("first_start_date_month")).orderBy("premium_quintile", "id").rowsBetween(-3, -1)
w_prev_start_date_month_produc = Window.partitionBy(F.month("prev_start_date"), "product")
w_premium_quintile_first_start_3 = Window.partitionBy("premium_quintile", F.year("first_start_date_month")).orderBy("premium_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_policies
    .filter((~F.col("status").isin("EXPIRED", "CANCELLED")) | (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("status").over(w_end_date_month_product)).alias("distinct_status_in_end_date_month_product"),
        F.min("premium").over(w_holder_id_by_premium_running).alias("min_premium_so_far"),
        F.first("start_date").over(w_holder_id_product_by_start_d).alias("first_start_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month_product), 2).alias("premium_vs_end_date_month_product_avg"),
        F.ntile(5).over(w_holder_id_by_premium).alias("premium_quintile"),
        F.lag("start_date").over(w_holder_id_by_premium).alias("prev_start_date"),
        F.lead("premium", 2, 0).over(w_product_by_end_date).alias("next2_premium"),
        F.nth_value("premium", 2).over(w_product_by_end_date_full).alias("second_premium"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_premium)).alias("premium_change"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_status), 2).alias("premium_vs_product_status_avg"),
    )
)

annotated = (
    ranked
    .filter(F.col("end_date") < F.lit("2026-08-31").cast("date"))
    .select(
        "*",
        F.date_trunc("month", F.col("first_start_date")).alias("first_start_date_month"),
        (F.floor(F.col("premium_change") / 5000) * 5000).alias("premium_change_bucket"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.max("premium_quintile").over(w_premium_quintile_first_start).alias("max_premium_quintile_so_far"),
        F.sum("min_premium_so_far").over(w_premium_quintile_first_start_2).alias("min_premium_so_far_sum_3_prior"),
        F.round(F.col("premium_quintile") - F.avg("premium_quintile").over(w_prev_start_date_month_produc), 2).alias("premium_quintile_vs_prev_start_date_month_produc"),
        F.first("next2_premium").over(w_premium_quintile_first_start_3).alias("first_next2_premium"),
        F.round(F.avg("premium").over(w_prev_start_date_month_produc), 2).alias("prev_start_date_month_product_avg_premium"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "end_date_label",
        "distinct_status_in_end_date_month_product",
        "first_start_date",
        "premium_quintile",
        "prev_start_date",
        "next2_premium",
        "second_premium",
        "premium_change",
        "premium_vs_product_status_avg",
        "premium_change_bucket",
        "max_premium_quintile_so_far",
        "min_premium_so_far_sum_3_prior",
        "premium_quintile_vs_prev_start_date_month_produc",
        "first_next2_premium",
        "prev_start_date_month_product_avg_premium",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
    .orderBy("start_date_month_start")
)
