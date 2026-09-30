from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month = Window.partitionBy(F.date_trunc("month", "holder_joined_on"))
w_all_by_end_date_prior_2 = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, -1)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_end_date_year_by_end_date = Window.partitionBy(F.year("end_date")).orderBy("end_date", "id")
w_all_by_end_date_moving_3 = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_status_by_start_date_full = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_year_by_end_date_ti = Window.partitionBy(F.year("end_date")).orderBy("end_date")
w_end_date_year_by_end_date_mo = Window.partitionBy(F.year("end_date")).orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w_all_by_end_date_running = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_year_by_end_date_ru = Window.partitionBy(F.year("end_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("holder_id") <= 15)
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

joined_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
    )
)

with_trends = (
    joined_holder
    .select(
        "*",
        F.size(F.collect_set("status").over(w_holder_joined_on_month)).alias("distinct_status_in_holder_joined_on_month"),
        F.round(F.avg("holder_risk_score").over(w_all_by_end_date_prior_2), 2).alias("holder_risk_score_avg_2_prior"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_joined_on_month), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_joined_on_mont"),
        F.min("holder_risk_score").over(w_holder_joined_on_month).alias("holder_joined_on_month_min_holder_risk_score"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_end_date_year_by_end_date)).alias("days_since_prev_holder_joined_on"),
        F.round(F.avg("holder_risk_score").over(w_all_by_end_date_moving_3), 2).alias("holder_risk_score_avg_4"),
        F.count(F.lit(1)).over(w_holder_joined_on_month).alias("rows_in_holder_joined_on_month"),
        F.last("start_date").over(w_status_by_start_date_full).alias("last_start_date"),
        F.round(F.cume_dist().over(w_end_date_year_by_end_date_ti), 3).alias("end_date_cume_dist"),
        F.round(F.avg("holder_risk_score").over(w_end_date_year_by_end_date_mo), 2).alias("holder_risk_score_avg_7"),
        F.sum("holder_risk_score").over(w_all_by_end_date_running).alias("running_holder_risk_score"),
        F.sum("holder_risk_score").over(w_end_date_year_by_end_date_ru).alias("running_holder_risk_score_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "holder_risk_score",
        "holder_name",
        "end_date_dense_rank",
        "holder_risk_score_share_of_holder_joined_on_mont",
        "rows_in_holder_joined_on_month",
        "last_start_date",
        "holder_risk_score_avg_7",
        "running_holder_risk_score",
        "running_holder_risk_score_2",
        F.least(F.col("holder_risk_score"), F.col("holder_risk_score_avg_4")).alias("least_holder_risk_score_holder_risk_score_avg_4"),
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
    )
    .orderBy(F.col("least_holder_risk_score_holder_risk_score_avg_4").asc())
)
