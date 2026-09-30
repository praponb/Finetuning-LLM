from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_prior_3 = Window.orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_start_date_month_holder_id_b = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_status_end_date_month = Window.partitionBy("status", F.date_trunc("month", "end_date"))
w_start_date_month_holder_id_b_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_all_by_start_date_moving_1 = Window.orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc_nulls_first())
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy(F.col("premium").desc(), "id")

scoped = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

metrics = (
    scoped
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.max("premium").over(w_all_by_start_date_prior_3).alias("premium_max_3_prior"),
        F.round(F.avg("premium").over(w_start_date_month_holder_id_b), 2).alias("premium_avg_7"),
        F.count(F.lit(1)).over(w_status_end_date_month).alias("rows_in_status_end_date_month"),
        F.last("end_date").over(w_start_date_month_holder_id_b_2).alias("last_end_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_month), F.lit(0)), 4).alias("premium_share_of_start_date_month"),
        F.sum("premium").over(w_start_date_month).alias("start_date_month_sum_premium"),
        F.max("premium").over(w_all_by_start_date_moving_1).alias("premium_max_2"),
        F.dense_rank().over(w_all_by_start_date_ties).alias("start_date_dense_rank"),
        F.lag("premium").over(w_product_status_by_premium).alias("prev_premium"),
    )
)

tagged = (
    metrics
    .select(
        "*",
        (F.floor(F.col("premium_max_3_prior") / 5000) * 5000).alias("premium_max_3_prior_bucket"),
        F.concat(F.col("status"), F.lit("#"), F.col("start_date_dense_rank").cast("string")).alias("status_ref"),
        F.when(F.col("status").isin("EXPIRED", "CANCELLED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

result = (
    tagged
    .select(
        "id",
        "product",
        "premium",
        "status",
        "premium_max_3_prior",
        "premium_avg_7",
        "rows_in_status_end_date_month",
        "premium_share_of_start_date_month",
        "start_date_month_sum_premium",
        "premium_max_2",
        "prev_premium",
        "premium_max_3_prior_bucket",
    )
    .orderBy("status", "premium_share_of_start_date_month")
)
