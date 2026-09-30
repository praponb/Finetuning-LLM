from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_by_start_da = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("start_date").desc_nulls_first(), "id")
w_start_date_plus_365d_month_s = Window.partitionBy(F.date_trunc("month", "start_date_plus_365d"), "status").orderBy(F.col("end_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_product_by_start_date_plus_3 = Window.partitionBy("product").orderBy("start_date_plus_365d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_by_start_da_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_product_by_start_date_plus_3_2 = Window.partitionBy("product").orderBy("start_date_plus_365d", "id")
w_product_end_date_month_by_pr = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy("premium", "id").rowsBetween(-2, -1)
w_start_date_plus_365d_month_s_2 = Window.partitionBy(F.date_trunc("month", "start_date_plus_365d"), "status").orderBy(F.col("end_date").desc(), "id")
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_last_first_start_date = Window.orderBy(F.col("last_first_start_date").desc())
w_distinct_holder_ids = Window.partitionBy("distinct_holder_ids")

scoped_policys = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-09-23").cast("date")) | (F.col("premium").between(918.1, 4906.3)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_add(F.col("start_date"), 365).alias("start_date_plus_365d"),
    )
)

windowed = (
    scoped_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        F.round(F.cume_dist().over(w_start_date_month_by_start_da), 3).alias("start_date_cume_dist"),
        F.max("premium").over(w_start_date_plus_365d_month_s).alias("premium_max_2"),
        F.first("start_date").over(w_product_by_start_date_plus_3).alias("first_start_date"),
        F.lag("end_date", 3).over(w_start_date_month_by_start_da).alias("prev3_end_date"),
        F.sum("premium").over(w_start_date_month_by_start_da_2).alias("premium_sum_2"),
        F.ntile(3).over(w_product_by_start_date_plus_3_2).alias("start_date_plus_365d_tercile"),
        F.sum("premium").over(w_product_end_date_month_by_pr).alias("premium_sum_2_prior"),
        F.ntile(4).over(w_start_date_plus_365d_month_s_2).alias("end_date_quartile"),
        F.round(F.percent_rank().over(w_start_date_month_by_start_da), 3).alias("start_date_percent_rank"),
        F.min("premium").over(w_status_holder_id).alias("status_holder_id_min_premium"),
    )
)

totals = (
    windowed
    .filter(F.col("premium_sum_2") < 10560.0)
    .groupBy(
        "holder_id",
        F.date_trunc("month", "first_start_date").alias("first_start_date_month"),
    )
    .agg(
        F.max("first_start_date").alias("last_first_start_date"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.round(F.sum("start_date_percent_rank") / F.nullif(F.sum("premium_sum_2"), F.lit(0)), 3).alias("start_date_percent_rank_to_premium_sum_2_ratio"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(100 * F.count(F.when(~F.col("product").isin("HOME", "TRAVEL"), 1)) / F.count("*"), 1).alias("pct_product_not_in_home_travel"),
    )
)

windowed_2 = (
    totals
    .select(
        "*",
        F.dense_rank().over(w_all_by_last_first_start_date).alias("last_first_start_date_dense_rank"),
        F.max("n_rows").over(w_distinct_holder_ids).alias("distinct_holder_ids_max_n_rows"),
    )
)

result = (
    windowed_2
    .select(
        "holder_id",
        "first_start_date_month",
        "n_rows",
        "total_premium",
        "start_date_percent_rank_to_premium_sum_2_ratio",
        "distinct_holder_ids",
        "pct_product_not_in_home_travel",
        "last_first_start_date_dense_rank",
        "distinct_holder_ids_max_n_rows",
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
    )
    .orderBy(F.col("first_start_date_month").asc())
)
