from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_policy_premium = Window.partitionBy("policy_id").orderBy("policy_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_status_reported_at_to = Window.partitionBy("policy_status", F.to_date("reported_at")).orderBy(F.col("days_since_reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_status_reported_at_to_2 = Window.partitionBy("policy_status", F.to_date("reported_at")).orderBy(F.col("days_since_reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_reported_at_to_date_by_polic = Window.partitionBy(F.to_date("reported_at")).orderBy(F.col("policy_premium").desc(), "id")
w_policy_id_by_policy_premium_2 = Window.partitionBy("policy_id").orderBy("policy_premium", "id")
w_policy_status = Window.partitionBy("policy_status")
w_adjuster_id_by_policy_start = Window.partitionBy("adjuster_id").orderBy("policy_start_date", "id")
w_adjuster_id_by_policy_premiu = Window.partitionBy("adjuster_id").orderBy("policy_premium_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_status_policy_id = Window.partitionBy("policy_status", "policy_id")
w_first_policy_start_date_mont = Window.partitionBy(F.month("first_policy_start_date"), F.date_trunc("month", "policy_start_date")).orderBy("rn", "id")
w_cnt_policy_start_date_tercil = Window.partitionBy("cnt", "policy_start_date_tercile").orderBy(F.col("cnt").asc_nulls_last(), "policy_start_date_tercile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cnt_policy_start_date_tercil_2 = Window.partitionBy("cnt", "policy_start_date_tercile")

scoped = (
    df_claims
    .filter(F.col("claimed_amount") >= 19609.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.days_since_reported_at",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.nth_value("days_since_reported_at", 2).over(w_policy_id_by_policy_premium).alias("second_days_since_reported_at"),
        F.first("policy_start_date").over(w_policy_status_reported_at_to).alias("first_policy_start_date"),
        F.min("days_since_reported_at").over(w_policy_status_reported_at_to_2).alias("min_days_since_reported_at_so_far"),
        F.max("policy_premium").over(w_adjuster_id).alias("adjuster_id_max_policy_premium"),
        F.row_number().over(w_reported_at_to_date_by_polic).alias("rn"),
        F.round(F.percent_rank().over(w_policy_id_by_policy_premium_2), 3).alias("policy_premium_percent_rank"),
        F.round(F.avg("days_since_reported_at").over(w_policy_status), 2).alias("policy_status_avg_days_since_reported_at"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        F.greatest(F.col("policy_premium"), F.col("rn")).alias("greatest_policy_premium_rn"),
        F.trunc(F.col("policy_start_date"), "MM").alias("policy_start_date_month_start"),
    )
)

sequenced_2 = (
    scored
    .select(
        "*",
        F.ntile(3).over(w_adjuster_id_by_policy_start).alias("policy_start_date_tercile"),
        F.sum("greatest_policy_premium_rn").over(w_adjuster_id_by_policy_premiu).alias("running_greatest_policy_premium_rn"),
        F.round(F.col("policy_premium_percent_rank") / F.nullif(F.sum("policy_premium_percent_rank").over(w_policy_status_policy_id), F.lit(0)), 4).alias("policy_premium_percent_rank_share_of_policy_stat"),
        F.round(F.col("greatest_policy_premium_rn") - F.avg("greatest_policy_premium_rn").over(w_policy_status_policy_id), 2).alias("greatest_policy_premium_rn_vs_policy_status_poli"),
        (F.col("policy_premium") - F.lag("policy_premium").over(w_first_policy_start_date_mont)).alias("policy_premium_change"),
    )
)

summary = (
    sequenced_2
    .groupBy("policy_start_date_tercile")
    .agg(
        F.array_sort(F.collect_set("policy_status")).alias("policy_status_list"),
        F.sum("greatest_policy_premium_rn").alias("total_greatest_policy_premium_rn"),
        F.count("*").alias("cnt"),
    )
)

sequenced_3 = (
    summary
    .select(
        "*",
        F.sum("policy_start_date_tercile").over(w_cnt_policy_start_date_tercil).alias("running_policy_start_date_tercile"),
        F.count(F.lit(1)).over(w_cnt_policy_start_date_tercil_2).alias("rows_in_cnt_policy_start_date_tercile"),
    )
)

result = (
    sequenced_3
    .select(
        "policy_start_date_tercile",
        "policy_status_list",
        "cnt",
        "running_policy_start_date_tercile",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
        F.abs(F.col("cnt") - 9).alias("cnt_distance"),
    )
)
