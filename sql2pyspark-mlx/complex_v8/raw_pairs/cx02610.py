from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_product_by = Window.partitionBy(F.date_trunc("month", "start_date"), "product").orderBy("premium", "id")
w_holder_id = Window.partitionBy("holder_id")
w_product_start_date_year = Window.partitionBy("product", F.year("start_date"))
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_holder_id_by_premium_ties = Window.partitionBy("holder_id").orderBy("premium")
w_all_by_premium_moving_3 = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_premium_prior_6 = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_start_date_month_product_by_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_start_date_month_product_by_3 = Window.partitionBy(F.date_trunc("month", "start_date"), "product").orderBy("premium", "id").rowsBetween(-1, Window.currentRow)

cleaned_policys = (
    df_policies
    .filter(~F.col("status").isin("LAPSED", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

ranked = (
    cleaned_policys
    .select(
        "*",
        F.rank().over(w_start_date_month_product_by).alias("premium_rank"),
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.size(F.collect_set("holder_id").over(w_product_start_date_year)).alias("distinct_holder_id_in_product_start_date_year"),
        F.nth_value("end_date", 2).over(w_all_by_end_date_full).alias("second_end_date"),
        F.lead("start_date").over(w_holder_id_by_premium).alias("next_start_date"),
        F.ntile(3).over(w_holder_id_by_premium).alias("premium_tercile"),
        F.dense_rank().over(w_holder_id_by_premium_ties).alias("premium_dense_rank"),
        F.max("premium").over(w_all_by_premium_moving_3).alias("premium_max_4"),
        F.round(F.avg("premium").over(w_all_by_premium_prior_6), 2).alias("premium_avg_6_prior"),
        F.first("start_date").over(w_start_date_month_product_by_2).alias("first_start_date"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.lag("start_date").over(w_all_by_premium).alias("prev_start_date"),
        F.round(F.cume_dist().over(w_holder_id_by_premium_ties), 3).alias("premium_cume_dist"),
        F.lead("end_date", 2).over(w_holder_id_by_premium).alias("next2_end_date"),
        F.sum("premium").over(w_start_date_month_product_by_3).alias("premium_sum_2"),
        F.nth_value("premium", 2).over(w_all_by_end_date_full).alias("second_premium"),
    )
)

tagged = (
    ranked
    .filter(F.col("premium_cume_dist").between(0.1, 0.7))
    .select(
        "*",
        F.round(F.col("premium_avg_6_prior") / F.nullif(F.col("premium_tercile"), F.lit(0)), 3).alias("premium_avg_6_prior_per_premium_tercile"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "premium_rank",
        "second_end_date",
        "next_start_date",
        "premium_tercile",
        "premium_dense_rank",
        "premium_max_4",
        "premium_cume_dist",
        "next2_end_date",
        "second_premium",
        "premium_avg_6_prior_per_premium_tercile",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
        F.abs(F.col("premium_dense_rank") - 6).alias("premium_dense_rank_distance"),
    )
)
