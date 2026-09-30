from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_start_d = Window.partitionBy("holder_id", "product").orderBy("start_date")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy("premium", "id").rowsBetween(-2, -1)
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_product_holder_id_by_premium_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), "id")
w_product_holder_id_by_premium_3 = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_product_holder_id_by_premium_4 = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc())
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy("premium", "id")
w_status_holder_id_by_premium_3 = Window.partitionBy("status", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_rank_holder_id_by = Window.partitionBy("start_date_rank", "holder_id").orderBy("second_start_date")
w_all_by_premium_sum_2_prior = Window.orderBy(F.col("premium_sum_2_prior").desc_nulls_first(), "id")
w_status_group = Window.partitionBy("status_group")
w_start_date_rank_holder_id_by_2 = Window.partitionBy("start_date_rank", "holder_id").orderBy("second_start_date", "id").rowsBetween(-3, Window.currentRow)
w_first_start_date_month_by_pr = Window.partitionBy(F.month("first_start_date")).orderBy("premium_sum_2_prior")
w_all_by_max_premium_sum_2_pri = Window.orderBy(F.col("max_premium_sum_2_prior_cume_dist").desc_nulls_first(), "start_date_rank", "start_date_month", "second_start_date_rank")
w_all_by_max_premium_sum_2_pri_2 = Window.orderBy(F.col("max_premium_sum_2_prior_cume_dist").desc_nulls_first())

scoped = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.rank().over(w_holder_id_product_by_start_d).alias("start_date_rank"),
        F.sum("premium").over(w_status_holder_id_by_premium).alias("premium_sum_2_prior"),
        F.first("start_date").over(w_product_holder_id_by_premium).alias("first_start_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_product_holder_id_by_premium_2)).alias("days_since_prev_start_date"),
        F.sum("premium").over(w_product_holder_id_by_premium_3).alias("premium_sum_2"),
        F.round(F.cume_dist().over(w_product_holder_id_by_premium_4), 3).alias("premium_cume_dist"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.ntile(10).over(w_status_holder_id_by_premium_2).alias("premium_decile"),
        F.nth_value("start_date", 2).over(w_status_holder_id_by_premium_3).alias("second_start_date"),
    )
)

scored = (
    metrics
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "start_date_rank",
        "premium_sum_2_prior",
        "first_start_date",
        "premium_vs_holder_id_avg",
        "days_since_prev_start_date",
        "premium_cume_dist",
        "rows_in_holder_id",
        "second_start_date",
        F.concat(F.col("status"), F.lit("#"), F.col("start_date_rank").cast("string")).alias("status_ref"),
        F.when(F.col("status").isin("LAPSED", "EXPIRED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.quarter(F.col("second_start_date")).alias("second_start_date_quarter"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "start_date_rank",
        "premium_sum_2_prior",
        "premium_cume_dist",
        "rows_in_holder_id",
        "second_start_date",
        "status_ref",
        "status_group",
        "second_start_date_quarter",
        F.rank().over(w_start_date_rank_holder_id_by).alias("second_start_date_rank"),
        F.round(F.cume_dist().over(w_all_by_premium_sum_2_prior), 3).alias("premium_sum_2_prior_cume_dist"),
        F.round(F.cume_dist().over(w_start_date_rank_holder_id_by), 3).alias("second_start_date_cume_dist"),
        F.count(F.lit(1)).over(w_status_group).alias("rows_in_status_group"),
        F.round(F.avg("premium").over(w_start_date_rank_holder_id_by_2), 2).alias("premium_avg_4"),
        F.round(F.cume_dist().over(w_first_start_date_month_by_pr), 3).alias("premium_sum_2_prior_cume_dist_2"),
    )
)

per_group = (
    ranked
    .filter(F.col("status_ref").isNotNull())
    .groupBy(
        "start_date_rank",
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "second_start_date_rank",
    )
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.max("premium_sum_2_prior_cume_dist").alias("max_premium_sum_2_prior_cume_dist"),
        F.round(F.sum("premium_sum_2_prior_cume_dist") / F.nullif(F.sum("second_start_date_cume_dist"), F.lit(0)), 3).alias("premium_sum_2_prior_cume_dist_to_second_start_da"),
    )
)

metrics_2 = (
    per_group
    .select(
        "start_date_rank",
        "start_date_month",
        "second_start_date_rank",
        F.row_number().over(w_all_by_max_premium_sum_2_pri).alias("rn"),
        F.rank().over(w_all_by_max_premium_sum_2_pri_2).alias("max_premium_sum_2_prior_cume_dist_rank"),
        F.lead("start_date_rank", 3).over(w_all_by_max_premium_sum_2_pri).alias("next3_start_date_rank"),
    )
)

result = (
    metrics_2
    .select(
        "start_date_rank",
        "rn",
        "max_premium_sum_2_prior_cume_dist_rank",
        "next3_start_date_rank",
        F.abs(F.col("second_start_date_rank") - 5).alias("second_start_date_rank_distance"),
    )
    .orderBy("start_date_rank")
)
