from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_start_date_month_status = Window.partitionBy(F.month("start_date"), "status")
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy("start_date", "id")
w_all_by_start_date_prior_2 = Window.orderBy("start_date", "id").rowsBetween(-2, -1)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_status_holder_id_by_start_da_3 = Window.partitionBy("status", "holder_id").orderBy("start_date", "id").rowsBetween(-6, -1)
w_all_by_start_date_2 = Window.orderBy(F.col("start_date").desc(), "id")
w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_prior_6 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_end_date_month_by_end_date = Window.partitionBy(F.month("end_date")).orderBy(F.col("end_date").desc(), "id")
w_distinct_holder_id_in_start = Window.partitionBy("distinct_holder_id_in_start_date_month_status")
w_start_date_plus_12m_month_ho = Window.partitionBy(F.date_trunc("month", "start_date_plus_12m"), "holder_id").orderBy("min_days_since_start_date_so_far", "id")
w_start_date_plus_12m_month_ho_2 = Window.partitionBy(F.date_trunc("month", "start_date_plus_12m"), "holder_id").orderBy("min_days_since_start_date_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_plus_12m_month_ho_3 = Window.partitionBy(F.date_trunc("month", "start_date_plus_12m"), "holder_id").orderBy("min_days_since_start_date_so_far", "id").rowsBetween(-2, -1)

valid_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") == "AUTO"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

ranked = (
    valid_policys
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "start_date_plus_12m",
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_holder_id), 2).alias("days_since_start_date_vs_holder_id_avg"),
        F.size(F.collect_set("holder_id").over(w_start_date_month_status)).alias("distinct_holder_id_in_start_date_month_status"),
        F.first("start_date_plus_12m").over(w_status_holder_id_by_start_da).alias("first_start_date_plus_12m"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_holder_id_by_start_da_2)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.sum("days_since_start_date").over(w_all_by_start_date_prior_2).alias("days_since_start_date_sum_2_prior"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.max("days_since_start_date").over(w_status_holder_id_by_start_da_3).alias("days_since_start_date_max_6_prior"),
        F.round(F.col("days_since_start_date") / F.nullif(F.sum("days_since_start_date").over(w_start_date_month_status), F.lit(0)), 4).alias("days_since_start_date_share_of_start_date_month_"),
        F.lag("days_since_start_date").over(w_all_by_start_date_2).alias("prev_days_since_start_date"),
        F.min("days_since_start_date").over(w_end_date_year_by_start_date).alias("min_days_since_start_date_so_far"),
        F.sum("days_since_start_date").over(w_all_by_start_date_prior_6).alias("days_since_start_date_sum_6_prior"),
        F.sum("days_since_start_date").over(w_start_date_month_status).alias("start_date_month_status_sum_days_since_start_dat"),
        F.round(F.col("days_since_start_date") / F.nullif(F.sum("days_since_start_date").over(w_holder_id), F.lit(0)), 4).alias("days_since_start_date_share_of_holder_id"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "start_date_plus_12m",
        "distinct_holder_id_in_start_date_month_status",
        "days_since_start_date_sum_2_prior",
        "start_date_cume_dist",
        "days_since_start_date_max_6_prior",
        "min_days_since_start_date_so_far",
        "days_since_start_date_share_of_holder_id",
        F.datediff(F.col("first_start_date_plus_12m"), F.lag("first_start_date_plus_12m").over(w_end_date_month_by_end_date)).alias("days_since_prev_first_start_date_plus_12m"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
        F.count(F.lit(1)).over(w_distinct_holder_id_in_start).alias("rows_in_distinct_holder_id_in_start_date_month_s"),
        F.dense_rank().over(w_start_date_plus_12m_month_ho).alias("min_days_since_start_date_so_far_dense_rank"),
        F.first("start_date_cume_dist").over(w_start_date_plus_12m_month_ho_2).alias("first_start_date_cume_dist"),
        F.sum("prev_days_since_start_date").over(w_all_by_start_date_running).alias("running_prev_days_since_start_date"),
        F.round(F.avg("days_since_prev_start_date").over(w_start_date_plus_12m_month_ho_3), 2).alias("days_since_prev_start_date_avg_2_prior"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "distinct_holder_id_in_start_date_month_status",
    )
    .agg(
        F.round(F.sum("first_start_date_cume_dist"), 2).alias("total_first_start_date_cume_dist"),
        F.max("start_date_percent_rank").alias("max_start_date_percent_rank"),
        F.count("*").alias("cnt"),
        F.max("end_date").alias("last_end_date"),
        F.round(100 * F.count(F.when(~F.col("status").isin("CANCELLED", "ACTIVE"), 1)) / F.count("*"), 1).alias("pct_status_not_in_cancelled_acti"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    grouped
    .filter(F.col("max_start_date_percent_rank") <= 0.8)
    .select("cnt", "end_date_month", "max_start_date_percent_rank")
    .distinct()
    .orderBy("max_start_date_percent_rank")
)
