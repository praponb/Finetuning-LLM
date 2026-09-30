from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_start_date_mon = Window.partitionBy("holder_region", F.date_trunc("month", "start_date")).orderBy(F.col("holder_risk_score").desc())
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_holder_region_status_by_star = Window.partitionBy("holder_region", "status").orderBy(F.col("start_date").desc(), "id")
w_start_date_month_holder_id_b = Window.partitionBy(F.month("start_date"), "holder_id").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_region_status_by_prem = Window.partitionBy("holder_region", "status").orderBy(F.col("premium").desc(), "id")
w_status = Window.partitionBy("status")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_quintile_holder_r = Window.partitionBy("start_date_quintile", "holder_region")
w_all_by_start_date_quintile = Window.orderBy(F.col("start_date_quintile").desc_nulls_first(), "id")
w_all_by_holder_risk_score_run = Window.orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_start_date_qui = Window.partitionBy("holder_region", "start_date_quintile").orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_all_by_holder_risk_score_tie = Window.orderBy(F.col("holder_risk_score").desc())

scoped = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-04-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

enriched = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.length(F.trim(F.col("p.holder_name"))) >= 7), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.sum("holder_risk_score").over(w_status_by_start_date_running).alias("running_holder_risk_score"),
        F.round(F.percent_rank().over(w_holder_region_start_date_mon), 3).alias("holder_risk_score_percent_rank"),
        F.sum("holder_risk_score").over(w_start_date_month).alias("start_date_month_sum_holder_risk_score"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.lead("premium", 1, 0).over(w_holder_region_status_by_star).alias("next_premium"),
        F.ntile(5).over(w_start_date_month_holder_id_b).alias("start_date_quintile"),
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_holder_region_status_by_prem)).alias("holder_risk_score_change"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.nth_value("start_date", 2).over(w_all_by_premium_full).alias("second_start_date"),
    )
)

enriched_2 = (
    ranked
    .select(
        "id",
        "premium",
        "start_date",
        "holder_risk_score",
        "holder_region",
        "running_holder_risk_score",
        "start_date_month_sum_holder_risk_score",
        "rows_in_start_date_month",
        "start_date_quintile",
        "holder_risk_score_change",
        F.initcap(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
        (F.col("premium") - F.col("start_date_quintile")).alias("premium_minus_start_date_quintile"),
    )
)

ranked_2 = (
    enriched_2
    .select(
        "*",
        F.min("running_holder_risk_score").over(w_start_date_quintile_holder_r).alias("start_date_quintile_holder_region_min_running_ho"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_start_date_quintile_holder_r), F.lit(0)), 4).alias("holder_risk_score_share_of_start_date_quintile_h"),
        F.round(F.percent_rank().over(w_all_by_start_date_quintile), 3).alias("start_date_quintile_percent_rank"),
        F.lead("start_date_quintile", 2).over(w_all_by_start_date_quintile).alias("next2_start_date_quintile"),
        F.count(F.lit(1)).over(w_all_by_holder_risk_score_run).alias("running_count"),
        F.sum("start_date_quintile").over(w_holder_region_start_date_qui).alias("start_date_quintile_sum_6_prior"),
        F.rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_rank"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "premium",
        "start_date",
        "holder_region",
        "running_holder_risk_score",
        "start_date_month_sum_holder_risk_score",
        "rows_in_start_date_month",
        "start_date_quintile",
        "holder_risk_score_change",
        "holder_region_clean",
        "premium_minus_start_date_quintile",
        "start_date_quintile_holder_region_min_running_ho",
        "start_date_quintile_percent_rank",
        "next2_start_date_quintile",
        "running_count",
        "start_date_quintile_sum_6_prior",
        "holder_risk_score_rank",
        (F.col("holder_risk_score_rank") + F.col("holder_risk_score_change")).alias("holder_risk_score_rank_plus_holder_risk_score_ch"),
    )
    .orderBy(F.col("holder_risk_score_rank").asc_nulls_last())
)
