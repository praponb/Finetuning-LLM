from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_end_date_month_by_start_date = Window.partitionBy(F.month("end_date")).orderBy(F.col("start_date").desc(), "id")
w_holder_id_holder_region = Window.partitionBy("holder_id", "holder_region")
w_holder_region_by_holder_risk = Window.partitionBy("holder_region").orderBy("holder_risk_score", "id")
w_end_date_month_by_start_date_2 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_by_holder_risk_2 = Window.partitionBy("holder_region").orderBy("holder_risk_score", "id").rowsBetween(-1, Window.currentRow)
w_end_date_month_by_start_date_3 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("start_date", "id").rowsBetween(-6, -1)
w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").desc(), "id")
w_end_date_month_by_start_date_4 = Window.partitionBy(F.month("end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_by_start_date_5 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("start_date", "id")
w_holder_region_by_holder_risk_3 = Window.partitionBy("holder_region").orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_premium_fille = Window.orderBy(F.col("running_premium_filled").desc(), "id")
w_start_date_quintile_end_date = Window.partitionBy("start_date_quintile", "end_date_quintile").orderBy(F.col("next_start_date").desc(), "id")
w_next_start_date_month = Window.partitionBy(F.date_trunc("month", "next_start_date"))
w_start_date_quintile_end_date_2 = Window.partitionBy("start_date_quintile", "end_date_quintile").orderBy(F.col("next_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .filter((F.col("status") == "ACTIVE") | (F.year(F.col("end_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

joined_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    joined_holder
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "premium_filled",
        "holder_region",
        "holder_risk_score",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.lead("start_date").over(w_end_date_month_by_start_date).alias("next_start_date"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id_holder_region), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id_holder_regi"),
        F.ntile(10).over(w_holder_region_by_holder_risk).alias("holder_risk_score_decile"),
        F.last("end_date", ignorenulls=True).over(w_end_date_month_by_start_date_2).alias("last_known_end_date"),
        F.round(F.avg("holder_risk_score").over(w_holder_region_by_holder_risk_2), 2).alias("holder_risk_score_avg_2"),
        F.round(F.avg("premium_filled").over(w_end_date_month_by_start_date_3), 2).alias("premium_filled_avg_6_prior"),
        F.ntile(5).over(w_status_holder_id_by_end_date).alias("end_date_quintile"),
        F.last("end_date", ignorenulls=True).over(w_end_date_month_by_start_date_4).alias("last_known_end_date_2"),
        F.ntile(5).over(w_end_date_month_by_start_date_5).alias("start_date_quintile"),
        F.sum("premium_filled").over(w_holder_region_by_holder_risk_3).alias("running_premium_filled"),
        F.sum("holder_risk_score").over(w_holder_id_holder_region).alias("holder_id_holder_region_sum_holder_risk_score"),
    )
)

tagged = (
    windowed
    .withColumn("holder_region_status_label", F.concat_ws("|", F.col("holder_region"), F.col("status")))
)

with_windows = (
    tagged
    .select(
        "id",
        "start_date",
        "status",
        "premium_filled",
        "holder_region",
        "next_start_date",
        "holder_risk_score_decile",
        "premium_filled_avg_6_prior",
        "end_date_quintile",
        "start_date_quintile",
        "running_premium_filled",
        "holder_region_status_label",
        F.rank().over(w_all_by_running_premium_fille).alias("running_premium_filled_rank"),
        F.lag("rows_in_status").over(w_start_date_quintile_end_date).alias("prev_rows_in_status"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_running_premium_fille)).alias("days_since_prev_end_date"),
        F.round(F.avg("end_date_quintile").over(w_next_start_date_month), 2).alias("next_start_date_month_avg_end_date_quintile"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_next_start_date_month), F.lit(0)), 4).alias("holder_risk_score_share_of_next_start_date_month"),
        F.round(F.avg("running_premium_filled").over(w_next_start_date_month), 2).alias("next_start_date_month_avg_running_premium_filled"),
        F.first("running_premium_filled").over(w_start_date_quintile_end_date_2).alias("first_running_premium_filled"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "status",
        "premium_filled",
        "holder_region",
        "next_start_date",
        "holder_risk_score_decile",
        "end_date_quintile",
        "running_premium_filled",
        "next_start_date_month_avg_end_date_quintile",
        "holder_risk_score_share_of_next_start_date_month",
        "next_start_date_month_avg_running_premium_filled",
        "first_running_premium_filled",
    )
    .orderBy(
        F.col("holder_risk_score_share_of_next_start_date_month").desc(),
        "status",
        "id",
    )
    .limit(10)
)
