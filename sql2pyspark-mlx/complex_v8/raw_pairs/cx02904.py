from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month = Window.partitionBy(F.month("start_date"))
w_all_by_start_date_prior_1 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")

prepared = df_policies.filter(F.col("holder_id").isNotNull())

windowed = (
    prepared
    .select(
        "*",
        F.max("premium").over(w_start_date_month).alias("start_date_month_max_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_month), F.lit(0)), 4).alias("premium_share_of_start_date_month"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_1), 2).alias("premium_avg_1_prior"),
        F.sum("premium").over(w_start_date_month).alias("start_date_month_sum_premium"),
        F.ntile(5).over(w_all_by_start_date).alias("start_date_quintile"),
        F.last("premium").over(w_all_by_end_date_full).alias("last_premium"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
        F.dense_rank().over(w_all_by_start_date).alias("start_date_dense_rank"),
        F.row_number().over(w_all_by_premium).alias("premium_seq"),
    )
)

first_rows = windowed.filter(F.col("start_date_dense_rank") == 1)

result = (
    first_rows
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-03-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "start_date_month_max_premium",
        "premium_share_of_start_date_month",
        "premium_avg_1_prior",
        "start_date_month_sum_premium",
        "start_date_quintile",
        "last_premium",
        "end_date_dense_rank",
        "start_date_dense_rank",
        "premium_seq",
        F.when(F.col("product").isin("LIFE", "AUTO"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)
