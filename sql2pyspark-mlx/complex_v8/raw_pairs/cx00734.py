from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_start_date_month_product_by = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_holder_id_b = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy("end_date", "id")
w_holder_id_end_date_month_by_3 = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").asc_nulls_last())
w_start_date_month_product_by_2 = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_end_date_month_by_4 = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc_nulls_first(), "id")
w_start_date_month_product_by_3 = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_end_date_quintile = Window.partitionBy("end_date_quintile")
w_all_by_n_product_not_in_trav = Window.orderBy(F.col("n_product_not_in_travel_home").desc(), "prev_start_date_ym", "end_date_quintile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_product_not_in_trav_2 = Window.orderBy(F.col("n_product_not_in_travel_home").desc())

cleaned_policys = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2024)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

ranked = (
    cleaned_policys
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        F.lag("start_date").over(w_holder_id_end_date_month_by).alias("prev_start_date"),
        F.first("start_date", ignorenulls=True).over(w_start_date_month_product_by).alias("first_known_start_date"),
        F.dense_rank().over(w_holder_id_end_date_month_by).alias("start_date_dense_rank"),
        F.last("end_date").over(w_holder_id_end_date_month_by_2).alias("last_end_date"),
        F.ntile(5).over(w_start_date_month_holder_id_b).alias("end_date_quintile"),
        F.rank().over(w_holder_id_end_date_month_by_3).alias("start_date_rank"),
        F.last("end_date", ignorenulls=True).over(w_start_date_month_product_by).alias("last_known_end_date"),
        F.nth_value("end_date", 2).over(w_start_date_month_product_by_2).alias("second_end_date"),
        F.last("start_date", ignorenulls=True).over(w_holder_id_end_date_month_by_4).alias("last_known_start_date"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
        F.lag("end_date").over(w_start_date_month_product_by_3).alias("prev_end_date"),
    )
)

grouped = (
    ranked
    .groupBy(
        F.date_format("prev_start_date", "yyyy-MM").alias("prev_start_date_ym"),
        "end_date_quintile",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("product").isin("TRAVEL", "HOME"), 1).otherwise(0)).alias("n_product_not_in_travel_home"),
        F.datediff(F.max("second_end_date"), F.min("second_end_date")).alias("second_end_date_span_days"),
        F.round(F.stddev("start_date_rank"), 2).alias("stddev_start_date_rank"),
    )
)

sequenced = (
    grouped
    .select(
        "prev_start_date_ym",
        "end_date_quintile",
        "second_end_date_span_days",
        "stddev_start_date_rank",
        F.count(F.lit(1)).over(w_end_date_quintile).alias("rows_in_end_date_quintile"),
        F.round(F.col("n_product_not_in_travel_home") / F.nullif(F.sum("n_product_not_in_travel_home").over(w_end_date_quintile), F.lit(0)), 4).alias("n_product_not_in_travel_home_share_of_end_date_q"),
        F.nth_value("n_product_not_in_travel_home", 2).over(w_all_by_n_product_not_in_trav).alias("second_n_product_not_in_travel_home"),
        F.dense_rank().over(w_all_by_n_product_not_in_trav_2).alias("n_product_not_in_travel_home_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "prev_start_date_ym",
        "end_date_quintile",
        "second_end_date_span_days",
        "rows_in_end_date_quintile",
        "n_product_not_in_travel_home_share_of_end_date_q",
        "n_product_not_in_travel_home_dense_rank",
        (F.col("end_date_quintile") + F.col("stddev_start_date_rank")).alias("end_date_quintile_plus_stddev_start_date_rank"),
        F.length(F.trim(F.col("prev_start_date_ym"))).alias("prev_start_date_ym_len"),
    )
    .orderBy(
        F.col("prev_start_date_ym_len").asc(),
        F.col("n_product_not_in_travel_home_dense_rank").desc(),
    )
)
