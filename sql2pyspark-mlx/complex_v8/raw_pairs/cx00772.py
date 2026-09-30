from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.month("claim_date")).orderBy("policy_start_date", "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id_claim_date_month_2 = Window.partitionBy("adjuster_id", F.month("claim_date")).orderBy("policy_start_date", "id")
w_adjuster_id_claim_date_month_3 = Window.partitionBy("adjuster_id", F.month("claim_date")).orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_month = Window.partitionBy("policy_id", "claim_date_month")
w_adjuster_level_by_policy_sta = Window.partitionBy("adjuster_level").orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_least_approved_amount = Window.orderBy(F.col("least_approved_amount_claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_least_approved_amount_2 = Window.orderBy(F.col("least_approved_amount_claimed_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_least_approved_amount_3 = Window.orderBy(F.col("least_approved_amount_claimed_amount").desc(), "id")
w_policy_start_date_year_claim = Window.partitionBy(F.year("policy_start_date"), "claim_date_month")
w_policy_start_date_rank_by_di = Window.partitionBy("policy_start_date_rank").orderBy(F.col("distinct_adjuster_id_in_policy_id_claim_date_mon").desc(), "id")
w_policy_start_date_rank_by_di_2 = Window.partitionBy("policy_start_date_rank").orderBy(F.col("distinct_adjuster_id_in_policy_id_claim_date_mon").desc())
w_first_adjuster_level_adjuste = Window.partitionBy("first_adjuster_level", "adjuster_id").orderBy("claimed_amount_share_of_policy_id_claim_date_mon", "id")
w_policy_start_date_rank_by_di_3 = Window.partitionBy("policy_start_date_rank").orderBy(F.col("distinct_adjuster_id_in_policy_id_claim_date_mon").desc(), "id").rowsBetween(-3, -1)

valid = (
    df_claims
    .filter((F.col("status") != "PAID") & (F.col("claim_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-04-20").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

joined = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.level").alias("adjuster_level"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "adjuster_id",
        "claim_date_month",
        "least_approved_amount_claimed_amount",
        "policy_start_date",
        "adjuster_level",
        F.round(F.avg("adjuster_level").over(w_adjuster_id_claim_date_month), 2).alias("adjuster_level_avg_3"),
        F.rank().over(w_adjuster_id_claim_date_month_2).alias("policy_start_date_rank"),
        F.first("reported_at", ignorenulls=True).over(w_adjuster_id_claim_date_month_3).alias("first_known_reported_at"),
        F.round(F.col("claim_date_month") / F.nullif(F.sum("claim_date_month").over(w_policy_id_claim_date_month), F.lit(0)), 4).alias("claim_date_month_share_of_policy_id_claim_date_m"),
        F.first("adjuster_level").over(w_adjuster_level_by_policy_sta).alias("first_adjuster_level"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_claim_date_month)).alias("distinct_adjuster_id_in_policy_id_claim_date_mon"),
        F.nth_value("policy_start_date", 2).over(w_all_by_least_approved_amount).alias("second_policy_start_date"),
        F.round(F.avg("claim_date_month").over(w_all_by_least_approved_amount_2), 2).alias("claim_date_month_avg_3"),
        F.lag("claim_date").over(w_all_by_least_approved_amount_3).alias("prev_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id_claim_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id_claim_date_mon"),
        F.min("adjuster_level").over(w_policy_id_claim_date_month).alias("policy_id_claim_date_month_min_adjuster_level"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_date_month",
        "policy_start_date",
        "adjuster_level",
        "adjuster_level_avg_3",
        "policy_start_date_rank",
        "first_known_reported_at",
        "first_adjuster_level",
        "distinct_adjuster_id_in_policy_id_claim_date_mon",
        "second_policy_start_date",
        "prev_claim_date",
        "claimed_amount_share_of_policy_id_claim_date_mon",
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_start_date_year_claim), F.lit(0)), 4).alias("adjuster_level_share_of_policy_start_date_year_c"),
        F.lead("claim_date_month").over(w_policy_start_date_rank_by_di).alias("next_claim_date_month"),
        F.round(F.percent_rank().over(w_policy_start_date_rank_by_di_2), 3).alias("distinct_adjuster_id_in_policy_id_claim_date_mon_2"),
        F.sum("first_adjuster_level").over(w_policy_start_date_year_claim).alias("policy_start_date_year_claim_date_month_sum_firs"),
        F.count(F.lit(1)).over(w_policy_start_date_year_claim).alias("rows_in_policy_start_date_year_claim_date_month"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_first_adjuster_level_adjuste)).alias("adjuster_level_change"),
        F.max("claim_date_month").over(w_policy_start_date_rank_by_di_3).alias("claim_date_month_max_3_prior"),
    )
)

result = (
    metrics
    .filter(F.col("next_claim_date_month") >= 5)
    .select(
        "id",
        "adjuster_id",
        "claim_date_month",
        "policy_start_date",
        "adjuster_level",
        "adjuster_level_avg_3",
        "policy_start_date_rank",
        "first_known_reported_at",
        "first_adjuster_level",
        "distinct_adjuster_id_in_policy_id_claim_date_mon",
        "second_policy_start_date",
        "prev_claim_date",
        "adjuster_level_share_of_policy_start_date_year_c",
        "next_claim_date_month",
        "distinct_adjuster_id_in_policy_id_claim_date_mon_2",
        "rows_in_policy_start_date_year_claim_date_month",
        "claim_date_month_max_3_prior",
    )
    .orderBy(F.col("policy_start_date").desc(), F.col("first_adjuster_level").desc())
)
