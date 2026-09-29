from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium_full = Window.partitionBy("status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium_ties = Window.partitionBy("status").orderBy("premium")
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_all_by_holder_risk_score_run = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_holder_risk_sco = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_risk_score_tie = Window.orderBy("holder_risk_score")
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")
w_holder_id_by_holder_risk_sco_2 = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_holder_id_by_start_date_prio = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(-2, -1)

recent_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-01-20").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

with_details = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_prefix",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status_prefix",
        "holder_risk_score",
        F.last("start_date").over(w_status_by_premium_full).alias("last_start_date"),
        F.rank().over(w_status_by_premium_ties).alias("premium_rank"),
        F.round(F.avg("premium").over(w_end_date_month), 2).alias("end_date_month_avg_premium"),
        F.sum("holder_risk_score").over(w_all_by_holder_risk_score_run).alias("running_holder_risk_score"),
        F.max("premium").over(w_end_date_month).alias("end_date_month_max_premium"),
        F.sum("premium").over(w_holder_id_by_holder_risk_sco).alias("running_premium"),
        F.round(F.cume_dist().over(w_all_by_holder_risk_score_tie), 3).alias("holder_risk_score_cume_dist"),
        F.lead("start_date").over(w_all_by_holder_risk_score).alias("next_start_date"),
        F.last("holder_risk_score").over(w_holder_id_by_holder_risk_sco_2).alias("last_holder_risk_score"),
        F.rank().over(w_all_by_holder_risk_score).alias("holder_risk_score_rank"),
        F.row_number().over(w_status_by_premium).alias("premium_seq"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_end_date_month), F.lit(0)), 4).alias("holder_risk_score_share_of_end_date_month"),
        F.round(F.avg("premium").over(w_holder_id_by_start_date_prio), 2).alias("premium_avg_2_prior"),
        F.min("holder_risk_score").over(w_end_date_month).alias("end_date_month_min_holder_risk_score"),
    )
)

stats = (
    with_trends
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_date_ym"))
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("running_holder_risk_score") / F.nullif(F.sum("holder_risk_score_share_of_end_date_month"), F.lit(0)), 3).alias("running_holder_risk_score_to_holder_risk_score_s"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.sum("premium_seq").alias("total_premium_seq"),
    )
)

result = (
    stats
    .select(
        "start_date_ym",
        "n_rows",
        "running_holder_risk_score_to_holder_risk_score_s",
        "product_list",
        "total_premium_seq",
    )
    .orderBy(
        F.col("running_holder_risk_score_to_holder_risk_score_s").asc(),
        "start_date_ym",
    )
    .limit(10)
)
