from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_product_start_date_month = Window.partitionBy("product", F.date_trunc("month", "start_date"))
w_start_date_month_by_start_da = Window.partitionBy(F.month("start_date")).orderBy("start_date", "id")
w_start_date_year_status_by_st = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("start_date").desc(), "id").rowsBetween(-3, -1)
w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_status_by_st_2 = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_start_date_month_by_start_da_2 = Window.partitionBy(F.month("start_date")).orderBy("start_date", "id").rowsBetween(-1, -1)

base = (
    df_policies
    .filter(F.col("product") != "HOME")
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
    base
    .select(
        "*",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_start_date)).alias("days_since_prev_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_start_date_month), F.lit(0)), 4).alias("premium_share_of_product_start_date_month"),
        F.ntile(4).over(w_start_date_month_by_start_da).alias("start_date_quartile"),
        F.lag("premium", 2).over(w_start_date_month_by_start_da).alias("prev2_premium"),
        F.max("premium").over(w_start_date_year_status_by_st).alias("premium_max_3_prior"),
        F.size(F.collect_set("holder_id").over(w_product_start_date_month)).alias("distinct_holder_id_in_product_start_date_month"),
        F.first("premium").over(w_product_status_by_start_date).alias("first_premium"),
        F.sum("premium").over(w_product_start_date_month).alias("product_start_date_month_sum_premium"),
        F.round(F.avg("premium").over(w_start_date_year_status_by_st_2), 2).alias("premium_avg_6_prior"),
        F.count(F.lit(1)).over(w_product_start_date_month).alias("rows_in_product_start_date_month"),
        F.max("premium").over(w_start_date_month_by_start_da_2).alias("premium_max_1_prior"),
        F.min("premium").over(w_product_start_date_month).alias("product_start_date_month_min_premium"),
    )
)

result = (
    metrics
    .filter(F.col("product").isin("AUTO", "LIFE", "TRAVEL"))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "days_since_prev_start_date",
        "premium_share_of_product_start_date_month",
        "start_date_quartile",
        "prev2_premium",
        "premium_max_3_prior",
        "first_premium",
        "product_start_date_month_sum_premium",
        "premium_avg_6_prior",
        "rows_in_product_start_date_month",
        "premium_max_1_prior",
        "product_start_date_month_min_premium",
        (F.col("premium_max_1_prior") - F.col("prev2_premium")).alias("premium_max_1_prior_minus_prev2_premium"),
    )
    .orderBy(
        F.col("days_since_prev_start_date").desc(),
        "product_start_date_month_min_premium",
    )
)
