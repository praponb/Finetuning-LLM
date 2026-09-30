from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_full = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_all_by_premium_2 = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-3, Window.currentRow)
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_start_date_prio = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id")
w_prev3_start_date_year_by_pre = Window.partitionBy(F.year("prev3_start_date")).orderBy("premium_max_4")
w_prev3_start_date_year_by_pre_2 = Window.partitionBy(F.year("prev3_start_date")).orderBy("premium_max_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_rank_status = Window.partitionBy("premium_rank", "status")
w_prev3_start_date_month_start = Window.partitionBy(F.date_trunc("month", "prev3_start_date"), F.year("start_date_month_start")).orderBy(F.col("holder_id_status_min_premium").desc_nulls_first(), "id")

base_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-11-20").cast("date")) & (F.col("premium") >= 2096.6))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

with_trends = (
    base_policys
    .select(
        "*",
        F.nth_value("start_date", 2).over(w_all_by_premium_full).alias("second_start_date"),
        F.lag("premium").over(w_all_by_premium).alias("prev_premium"),
        F.lag("start_date", 3).over(w_all_by_premium_2).alias("prev3_start_date"),
        F.max("premium").over(w_status_holder_id_by_premium).alias("premium_max_4"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_status), 2).alias("premium_vs_holder_id_status_avg"),
        F.row_number().over(w_holder_id_by_start_date).alias("rn"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_status), F.lit(0)), 4).alias("premium_share_of_holder_id_status"),
        F.round(F.avg("premium").over(w_holder_id_by_start_date_prio), 2).alias("premium_avg_6_prior"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.min("premium").over(w_holder_id_status).alias("holder_id_status_min_premium"),
        F.rank().over(w_status_holder_id_by_premium_2).alias("premium_rank"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "second_start_date",
        "prev_premium",
        "prev3_start_date",
        "premium_max_4",
        "holder_id_max_premium",
        "rows_in_holder_id",
        "premium_avg_6_prior",
        "premium_share_of_holder_id",
        "holder_id_status_min_premium",
        "premium_rank",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_prev3_start_date_year_by_pre), 3).alias("premium_max_4_cume_dist"),
        F.nth_value("holder_id_status_min_premium", 2).over(w_prev3_start_date_year_by_pre_2).alias("second_holder_id_status_min_premium"),
        F.count(F.lit(1)).over(w_premium_rank_status).alias("rows_in_premium_rank_status"),
        F.rank().over(w_prev3_start_date_year_by_pre).alias("premium_max_4_rank"),
        F.lead("premium_max_4").over(w_prev3_start_date_month_start).alias("next_premium_max_4"),
        F.round(F.avg("premium_max_4").over(w_premium_rank_status), 2).alias("premium_rank_status_avg_premium_max_4"),
    )
)

grouped = (
    sequenced
    .groupBy(
        F.date_format("prev3_start_date", "yyyy-MM").alias("prev3_start_date_ym"),
        F.date_format("start_date_month_start", "yyyy-MM").alias("start_date_month_start_ym"),
        "status",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "prev3_start_date_ym",
        "start_date_month_start_ym",
        "status",
        "pct_status_is_not_null",
        "row_count",
        "distinct_ids",
    )
    .orderBy(F.col("prev3_start_date_ym").desc())
)
