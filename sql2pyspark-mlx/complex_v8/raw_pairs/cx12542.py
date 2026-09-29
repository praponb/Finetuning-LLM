from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date")).orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_reported_at_m = Window.partitionBy("adjuster_level", F.month("reported_at")).orderBy("claim_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_level_policy_id_by = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc())
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").desc())
w_reported_at_day_month = Window.partitionBy(F.month("reported_at_day"))
w_distinct_policy_id_in_adjust = Window.partitionBy("distinct_policy_id_in_adjuster_level").orderBy("total_claimed_amount", "reported_at_day", "distinct_policy_id_in_adjuster_level").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_claims
    .filter((F.col("claim_type").isin("fire", "delay", "medical")) & (F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

enriched_adjuster = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") <= 3)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_date_quarter",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

windowed = (
    enriched_adjuster
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id), 2).alias("claimed_amount_vs_adjuster_id_avg"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_id_claim_date_month).alias("second_adjuster_level"),
        F.sum("approved_amount").over(w_adjuster_level_reported_at_m).alias("running_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_adjuster_level)).alias("distinct_policy_id_in_adjuster_level"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.lag("reported_at").over(w_all_by_claimed_amount).alias("prev_reported_at"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_level), 2).alias("approved_amount_vs_adjuster_level_avg"),
        F.round(F.cume_dist().over(w_adjuster_level_policy_id_by), 3).alias("claimed_amount_cume_dist"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_level), 2).alias("claimed_amount_vs_adjuster_level_avg"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist_2"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "adjuster_level",
        "adjuster_name",
        "second_adjuster_level",
        "distinct_policy_id_in_adjuster_level",
        "claimed_amount_cume_dist",
        "claimed_amount_vs_adjuster_level_avg",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

per_group = (
    annotated
    .groupBy(
        F.to_date("reported_at").alias("reported_at_day"),
        "distinct_policy_id_in_adjuster_level",
    )
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.round(F.sum("claimed_amount_vs_adjuster_level_avg") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("claimed_amount_vs_adjuster_level_avg_to_claimed_"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.length(F.trim(F.col("adjuster_name"))) >= 8, F.col("approved_amount")).otherwise(0)).alias("approved_amount_length_trim_adjuster_name_8"),
    )
)

sequenced = (
    per_group
    .select(
        "reported_at_day",
        "distinct_policy_id_in_adjuster_level",
        "adjuster_name_list",
        "total_claimed_amount",
        "cnt",
        "approved_amount_length_trim_adjuster_name_8",
        F.max("cnt").over(w_reported_at_day_month).alias("reported_at_day_month_max_cnt"),
        F.min("cnt").over(w_distinct_policy_id_in_adjust).alias("min_cnt_so_far"),
        F.count(F.lit(1)).over(w_reported_at_day_month).alias("rows_in_reported_at_day_month"),
    )
)

result = (
    sequenced
    .select(
        "reported_at_day",
        "distinct_policy_id_in_adjuster_level",
        "adjuster_name_list",
        "cnt",
        "approved_amount_length_trim_adjuster_name_8",
        "min_cnt_so_far",
        "rows_in_reported_at_day_month",
    )
    .orderBy(
        F.col("distinct_policy_id_in_adjuster_level").desc(),
        F.col("rows_in_reported_at_day_month").desc(),
    )
)
