from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_segment_by_sta = Window.partitionBy("status", "holder_segment").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_holder_segment = Window.partitionBy("holder_segment")
w_holder_id = Window.partitionBy("holder_id")
w_status_holder_segment_by_sta_2 = Window.partitionBy("status", "holder_segment").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_segment_by_hol = Window.partitionBy("status", "holder_segment").orderBy("holder_risk_score", "id")
w_status_holder_segment_by_sta_3 = Window.partitionBy("status", "holder_segment").orderBy(F.col("start_date").desc())
w_status_holder_segment_by_sta_4 = Window.partitionBy("status", "holder_segment").orderBy("start_date", "id")
w_holder_segment_holder_id_by = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_status_holder_segment_by_sta_5 = Window.partitionBy("status", "holder_segment").orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_holder_id_by_2 = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy("premium_vs_holder_segment_avg")
w_status_by_last_known_premium = Window.partitionBy("status").orderBy("last_known_premium", "id")
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy("premium_vs_holder_segment_avg", "id")
w_all_by_holder_id_max_premium = Window.orderBy("holder_id_max_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_id_max_premium_2 = Window.orderBy("holder_id_max_premium", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2026-01-20").cast("date"), F.lit("2026-09-23").cast("date"))) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.last("premium", ignorenulls=True).over(w_status_holder_segment_by_sta).alias("last_known_premium"),
        F.round(F.cume_dist().over(w_status_by_start_date), 3).alias("start_date_cume_dist"),
        F.round(F.avg("premium").over(w_holder_segment), 2).alias("holder_segment_avg_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_segment), 2).alias("premium_vs_holder_segment_avg"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.sum("premium").over(w_status_holder_segment_by_sta_2).alias("running_premium"),
        F.ntile(3).over(w_status_holder_segment_by_hol).alias("holder_risk_score_tercile"),
        F.round(F.cume_dist().over(w_status_holder_segment_by_sta_3), 3).alias("start_date_cume_dist_2"),
        F.dense_rank().over(w_status_holder_segment_by_sta_4).alias("start_date_dense_rank"),
        F.sum("holder_risk_score").over(w_holder_segment_holder_id_by).alias("holder_risk_score_sum_7"),
        F.sum("premium").over(w_status_holder_segment_by_sta_5).alias("premium_sum_1_prior"),
        F.count(F.lit(1)).over(w_status_by_start_date_running).alias("running_count"),
        F.round(F.avg("premium").over(w_holder_segment_holder_id_by_2), 2).alias("premium_avg_3"),
    )
)

leaders = (
    ranked
    .filter(F.col("start_date_dense_rank") == 1)
    .drop("start_date_dense_rank")
)

sequenced = (
    leaders
    .select(
        "*",
        F.rank().over(w_status_holder_id_by_premium).alias("premium_vs_holder_segment_avg_rank"),
        F.lead("holder_id_max_premium", 2).over(w_status_by_last_known_premium).alias("next2_holder_id_max_premium"),
        F.min("holder_risk_score_sum_7").over(w_start_date_month).alias("start_date_month_min_holder_risk_score_sum_7"),
        F.rank().over(w_status_holder_id_by_premium_2).alias("premium_vs_holder_segment_avg_rank_2"),
        F.first("premium_sum_1_prior", ignorenulls=True).over(w_all_by_holder_id_max_premium).alias("first_known_premium_sum_1_prior"),
        F.sum("running_count").over(w_all_by_holder_id_max_premium_2).alias("running_count_sum_7"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "holder_risk_score",
        "holder_segment",
        "last_known_premium",
        "start_date_cume_dist",
        "holder_segment_avg_premium",
        "premium_vs_holder_segment_avg",
        "running_premium",
        "holder_risk_score_sum_7",
        "premium_sum_1_prior",
        "running_count",
        "premium_avg_3",
        "premium_vs_holder_segment_avg_rank",
        "next2_holder_id_max_premium",
        "premium_vs_holder_segment_avg_rank_2",
        "first_known_premium_sum_1_prior",
        "running_count_sum_7",
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
        F.round(F.col("premium_sum_1_prior") * 0.9, 2).alias("premium_sum_1_prior_adj"),
    )
    .orderBy("running_premium", F.col("holder_risk_score_sum_7").asc())
)
