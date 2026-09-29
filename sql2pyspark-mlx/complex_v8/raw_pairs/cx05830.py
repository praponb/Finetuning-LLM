from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_premium_mo = Window.partitionBy("product", "status").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_end_date_year_status = Window.partitionBy(F.year("end_date"), "status")
w_start_date_month_holder_id_b = Window.partitionBy(F.month("start_date"), "holder_id").orderBy("end_date", "id")
w_status_product_by_end_date = Window.partitionBy("status", "product").orderBy(F.col("end_date").desc(), "id")
w_product = Window.partitionBy("product")
w_start_date_month_holder_id_b_2 = Window.partitionBy(F.month("start_date"), "holder_id").orderBy("end_date", "id").rowsBetween(-6, -1)
w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy("start_date", "id")
w_status_by_premium_ties = Window.partitionBy("status").orderBy(F.col("premium").asc_nulls_last())
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").asc_nulls_last(), "id")
w_distinct_product_in_end_date = Window.partitionBy("distinct_product_in_end_date_year_status").orderBy("distinct_product_in_end_date_year_status", "id").rowsBetween(-2, -1)
w_start_date_quartile_by_disti = Window.partitionBy("start_date_quartile").orderBy(F.col("distinct_holder_id_in_end_date_year_status").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_end_date_year_by_distinct_pr = Window.partitionBy(F.year("end_date")).orderBy("distinct_product_in_end_date_year_status", "id")

filtered = (
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.max("premium").over(w_product_status_by_premium_mo).alias("premium_max_2"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_year_status), F.lit(0)), 4).alias("premium_share_of_end_date_year_status"),
        F.size(F.collect_set("holder_id").over(w_end_date_year_status)).alias("distinct_holder_id_in_end_date_year_status"),
        F.size(F.collect_set("product").over(w_end_date_year_status)).alias("distinct_product_in_end_date_year_status"),
        F.round(F.percent_rank().over(w_start_date_month_holder_id_b), 3).alias("end_date_percent_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_product_by_end_date)).alias("days_since_prev_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.round(F.avg("premium").over(w_start_date_month_holder_id_b_2), 2).alias("premium_avg_6_prior"),
        F.ntile(4).over(w_product_status_by_start_date).alias("start_date_quartile"),
        F.round(F.cume_dist().over(w_status_by_premium_ties), 3).alias("premium_cume_dist"),
        F.count(F.lit(1)).over(w_end_date_year_status).alias("rows_in_end_date_year_status"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_premium)).alias("days_since_prev_start_date_2"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.sum("premium_max_2").over(w_distinct_product_in_end_date).alias("premium_max_2_sum_2_prior"),
        F.first("premium_share_of_product").over(w_start_date_quartile_by_disti).alias("first_premium_share_of_product"),
        F.round(F.col("end_date_percent_rank") / F.nullif(F.sum("end_date_percent_rank").over(w_end_date_month), F.lit(0)), 4).alias("end_date_percent_rank_share_of_end_date_month"),
        F.lead("premium").over(w_end_date_year_by_distinct_pr).alias("next_premium"),
        F.round(F.avg("premium_share_of_product").over(w_end_date_month), 2).alias("end_date_month_avg_premium_share_of_product"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.round(F.avg("rows_in_end_date_year_status").over(w_end_date_month), 2).alias("end_date_month_avg_rows_in_end_date_year_status"),
    )
)

result = (
    sequenced_2
    .filter(F.col("premium_max_2_sum_2_prior") <= 40520.0)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status_slug",
        "premium_max_2",
        "premium_share_of_end_date_year_status",
        "distinct_holder_id_in_end_date_year_status",
        "end_date_percent_rank",
        "premium_share_of_product",
        "premium_avg_6_prior",
        "start_date_quartile",
        "rows_in_end_date_year_status",
        "days_since_prev_start_date_2",
        "product_max_premium",
        "distinct_holder_id_in_product",
        "premium_max_2_sum_2_prior",
        "first_premium_share_of_product",
        "end_date_percent_rank_share_of_end_date_month",
        "rows_in_end_date_month",
        "end_date_month_avg_rows_in_end_date_year_status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("end_date_percent_rank_share_of_end_date_month").desc())
)
