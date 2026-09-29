from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date_full = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date = Window.partitionBy("status").orderBy("end_date", "id")
w_holder_id = Window.partitionBy("holder_id")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_running = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_distinct_status_in_holder_id = Window.partitionBy("distinct_status_in_holder_id", F.date_trunc("month", "second_end_date")).orderBy(F.col("first_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_end_date_year = Window.partitionBy(F.year("second_end_date"))
w_all_by_holder_id_min_premium = Window.orderBy(F.col("holder_id_min_premium").desc(), "id")
w_distinct_status_in_holder_id_2 = Window.partitionBy("distinct_status_in_holder_id", F.date_trunc("month", "second_end_date")).orderBy(F.col("first_premium").desc(), "id")

filtered_policys = (
    df_policies
    .filter((~F.col("status").isin("ACTIVE", "EXPIRED")) & (F.col("holder_id") >= 17))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

with_windows = (
    filtered_policys
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        F.nth_value("end_date", 2).over(w_status_by_end_date_full).alias("second_end_date"),
        (F.col("premium") - F.lag("premium").over(w_status_by_end_date)).alias("premium_change"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.nth_value("premium", 2).over(w_all_by_end_date_full).alias("second_premium"),
        F.size(F.collect_set("status").over(w_holder_id)).alias("distinct_status_in_holder_id"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.first("end_date").over(w_all_by_end_date_full).alias("first_end_date"),
        F.sum("premium").over(w_status_by_end_date_running).alias("running_premium"),
        F.first("premium").over(w_status_by_end_date_full).alias("first_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.sum("premium").over(w_all_by_end_date_running).alias("running_premium_2"),
        F.lag("premium").over(w_status_by_premium).alias("prev_premium"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.nth_value("holder_id_min_premium", 2).over(w_distinct_status_in_holder_id).alias("second_holder_id_min_premium"),
        F.round(F.col("distinct_status_in_holder_id") - F.avg("distinct_status_in_holder_id").over(w_second_end_date_year), 2).alias("distinct_status_in_holder_id_vs_second_end_date_"),
        F.max("premium_vs_holder_id_avg").over(w_second_end_date_year).alias("second_end_date_year_max_premium_vs_holder_id_av"),
        F.row_number().over(w_all_by_holder_id_min_premium).alias("holder_id_min_premium_seq"),
        F.rank().over(w_distinct_status_in_holder_id_2).alias("first_premium_rank"),
        F.lead("running_premium", 2).over(w_all_by_holder_id_min_premium).alias("next2_running_premium"),
        F.max("premium_change").over(w_second_end_date_year).alias("second_end_date_year_max_premium_change"),
    )
)

totals = (
    sequenced
    .filter(F.col("premium") <= 3532.7)
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product").alias("distinct_products"),
        F.min("premium").alias("min_premium"),
        F.max("second_end_date_year_max_premium_change").alias("max_second_end_date_year_max_premium_change"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "end_date_ym",
        "distinct_ids",
        "distinct_products",
        "min_premium",
        "max_second_end_date_year_max_premium_change",
        "n_rows",
        (F.floor(F.col("distinct_products") / 2) * 2).alias("distinct_products_bucket"),
        F.round(F.col("distinct_products") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("distinct_products_per_distinct_ids"),
    )
    .orderBy(F.col("distinct_products_per_distinct_ids").desc())
)
