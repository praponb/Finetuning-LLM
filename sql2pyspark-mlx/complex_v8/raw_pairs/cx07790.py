from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_end_date_month_status_by_day = Window.partitionBy(F.date_trunc("month", "end_date"), "status").orderBy(F.col("days_since_start_date").desc())
w_end_date_month_status_by_day_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "status").orderBy(F.col("days_since_start_date").desc(), "id")
w_status_by_days_since_start_d = Window.partitionBy("status").orderBy("days_since_start_date", "id")
w_all_by_days_since_start_date = Window.orderBy(F.col("days_since_start_date").desc(), "id")
w_premium_tier = Window.partitionBy("premium_tier")
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").desc(), "id")
w_all_by_days_since_start_date_2 = Window.orderBy(F.col("days_since_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_risk_score_ful = Window.orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_policies
    .filter(F.col("holder_id") <= 8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3817.5, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.premium_tier",
        "s.days_since_start_date",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "premium_tier",
        "days_since_start_date",
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_status), 2).alias("days_since_start_date_vs_status_avg"),
        F.round(F.cume_dist().over(w_end_date_month_status_by_day), 3).alias("days_since_start_date_cume_dist"),
        F.lag("end_date", 2).over(w_end_date_month_status_by_day_2).alias("prev2_end_date"),
        F.round(F.percent_rank().over(w_status_by_days_since_start_d), 3).alias("days_since_start_date_percent_rank"),
        F.lag("holder_risk_score", 3).over(w_all_by_days_since_start_date).alias("prev3_holder_risk_score"),
        F.max("holder_risk_score").over(w_premium_tier).alias("premium_tier_max_holder_risk_score"),
        F.lead("days_since_start_date", 2).over(w_all_by_holder_risk_score).alias("next2_days_since_start_date"),
        F.round(F.avg("holder_risk_score").over(w_status), 2).alias("status_avg_holder_risk_score"),
        F.first("days_since_start_date", ignorenulls=True).over(w_all_by_days_since_start_date_2).alias("first_known_days_since_start_date"),
        F.count(F.lit(1)).over(w_premium_tier).alias("rows_in_premium_tier"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_premium_tier), F.lit(0)), 4).alias("holder_risk_score_share_of_premium_tier"),
        F.first("end_date").over(w_all_by_holder_risk_score_ful).alias("first_end_date"),
    )
)

result = (
    metrics
    .select(
        "next2_days_since_start_date",
        "premium_tier",
        "days_since_start_date_cume_dist",
    )
    .distinct()
    .orderBy("days_since_start_date_cume_dist")
)
