from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_capacity = Window.orderBy(F.col("capacity").desc(), "id")
w_opened_on_month_by_opened_on = Window.partitionBy(F.month("opened_on"))
w_opened_on_month_by_capacity = Window.partitionBy(F.date_trunc("month", "opened_on"))
w_all_by_capacity_2 = Window.orderBy("capacity", "id")
w_opened_on_month_by_capacity_2 = Window.partitionBy(F.date_trunc("month", "opened_on")).orderBy(F.col("capacity").desc(), "id").rowsBetween(-1, -1)
w_all_by_days_since_opened_on = Window.orderBy(F.col("days_since_opened_on").asc_nulls_last(), "id")
w_opened_on_month_by_capacity_3 = Window.partitionBy(F.date_trunc("month", "opened_on")).orderBy(F.col("capacity").desc(), "id")
w_opened_on_month_by_opened_on_2 = Window.partitionBy(F.month("opened_on")).orderBy(F.col("opened_on").desc(), "id")
w_all_by_capacity_avg_1_prior = Window.orderBy("capacity_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_capacity_quintile_rn_2_by_ro = Window.partitionBy("capacity_quintile", "rn_2").orderBy("rows_in_opened_on_month", "id")
w_capacity_seq = Window.partitionBy("capacity_seq")
w_capacity_quintile_rn_2_by_ro_2 = Window.partitionBy("capacity_quintile", "rn_2").orderBy("rows_in_opened_on_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_wh_code_is_not_null_rn_3_b = Window.partitionBy("n_wh_code_is_not_null", "rn_3").orderBy("cnt", "opened_on_month", "prev3_opened_on_ym", "rn_3").rowsBetween(-2, Window.currentRow)
w_all_by_n_wh_code_is_not_null = Window.orderBy(F.col("n_wh_code_is_not_null").desc(), "opened_on_month", "prev3_opened_on_ym", "rn_3")
w_all_by_n_wh_code_is_not_null_2 = Window.orderBy(F.col("n_wh_code_is_not_null").desc(), "opened_on_month", "prev3_opened_on_ym", "rn_3").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "MMM yyyy").alias("opened_on_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.lead("days_since_opened_on").over(w_all_by_capacity).alias("next_days_since_opened_on"),
        F.count(F.lit(1)).over(w_opened_on_month_by_opened_on).alias("rows_in_opened_on_month"),
        F.max("capacity").over(w_opened_on_month_by_opened_on).alias("opened_on_month_max_capacity"),
        F.count(F.lit(1)).over(w_opened_on_month_by_capacity).alias("rows_in_opened_on_month_2"),
        F.row_number().over(w_all_by_capacity_2).alias("capacity_seq"),
        F.round(F.avg("capacity").over(w_opened_on_month_by_capacity_2), 2).alias("capacity_avg_1_prior"),
        F.lead("days_since_opened_on").over(w_all_by_days_since_opened_on).alias("next_days_since_opened_on_2"),
        F.round(F.percent_rank().over(w_all_by_capacity), 3).alias("capacity_percent_rank"),
        F.max("days_since_opened_on").over(w_opened_on_month_by_capacity).alias("opened_on_month_max_days_since_opened_on"),
        F.row_number().over(w_opened_on_month_by_capacity_3).alias("rn"),
        F.row_number().over(w_opened_on_month_by_opened_on_2).alias("rn_2"),
        F.ntile(5).over(w_all_by_capacity_2).alias("capacity_quintile"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "days_since_opened_on",
        "next_days_since_opened_on",
        "rows_in_opened_on_month",
        "capacity_avg_1_prior",
        "capacity_percent_rank",
        "opened_on_month_max_days_since_opened_on",
        F.last("next_days_since_opened_on_2").over(w_all_by_capacity_avg_1_prior).alias("last_next_days_since_opened_on_2"),
        F.lag("opened_on", 3).over(w_capacity_quintile_rn_2_by_ro).alias("prev3_opened_on"),
        F.round(F.col("capacity_percent_rank") - F.avg("capacity_percent_rank").over(w_capacity_seq), 2).alias("capacity_percent_rank_vs_capacity_seq_avg"),
        F.min("rows_in_opened_on_month").over(w_capacity_seq).alias("capacity_seq_min_rows_in_opened_on_month"),
        F.row_number().over(w_capacity_quintile_rn_2_by_ro).alias("rn_3"),
        (F.col("capacity_avg_1_prior") - F.lag("capacity_avg_1_prior").over(w_capacity_quintile_rn_2_by_ro)).alias("capacity_avg_1_prior_change"),
        F.nth_value("capacity_avg_1_prior", 2).over(w_capacity_quintile_rn_2_by_ro_2).alias("second_capacity_avg_1_prior"),
    )
)

rollup_base = (
    metrics
    .filter(F.col("last_next_days_since_opened_on_2") <= 455)
    .groupBy(
        F.date_trunc("month", "opened_on").alias("opened_on_month"),
        F.date_format("prev3_opened_on", "yyyy-MM").alias("prev3_opened_on_ym"),
        "rn_3",
    )
    .agg(
        F.sum(F.when(F.col("wh_code").isNotNull(), 1).otherwise(0)).alias("n_wh_code_is_not_null"),
        F.count("*").alias("cnt"),
        F.round(F.avg("capacity"), 2).alias("avg_capacity"),
        F.sum(F.when(F.length(F.trim(F.col("wh_code"))) >= 7, F.col("capacity_percent_rank_vs_capacity_seq_avg")).otherwise(0)).alias("capacity_percent_rank_vs_capacity_seq_avg_length"),
    )
    .filter(F.col("cnt") >= 3)
)

metrics_2 = (
    rollup_base
    .select(
        "opened_on_month",
        "prev3_opened_on_ym",
        "rn_3",
        "cnt",
        "avg_capacity",
        F.sum("avg_capacity").over(w_n_wh_code_is_not_null_rn_3_b).alias("avg_capacity_sum_3"),
        F.row_number().over(w_all_by_n_wh_code_is_not_null).alias("rn"),
        F.ntile(5).over(w_all_by_n_wh_code_is_not_null).alias("n_wh_code_is_not_null_quintile"),
        F.first("n_wh_code_is_not_null", ignorenulls=True).over(w_all_by_n_wh_code_is_not_null_2).alias("first_known_n_wh_code_is_not_null"),
    )
)

result = (
    metrics_2
    .select(
        "opened_on_month",
        "prev3_opened_on_ym",
        "rn_3",
        "avg_capacity_sum_3",
        "n_wh_code_is_not_null_quintile",
        F.lower(F.regexp_replace(F.trim(F.col("prev3_opened_on_ym")), " ", "_")).alias("prev3_opened_on_ym_slug"),
        F.abs(F.col("cnt") - 5).alias("cnt_distance"),
    )
    .orderBy(F.col("opened_on_month").desc(), "prev3_opened_on_ym", "rn_3")
    .limit(10)
)
