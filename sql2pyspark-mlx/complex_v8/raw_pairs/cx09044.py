from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_level = Window.partitionBy("policy_id", "adjuster_level")
w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("adjuster_level").desc())
w_all_by_policy_premium = Window.orderBy("policy_premium", "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_adjuster_level_runnin = Window.orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

with_details_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_windows = (
    with_details_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "adjuster_team",
        "adjuster_level",
        "policy_product",
        "policy_premium",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_adjuster_level), 2).alias("approved_amount_vs_policy_id_adjuster_level_avg"),
        F.rank().over(w_policy_id_claim_date_month_b).alias("adjuster_level_rank"),
        F.lead("claimed_amount", 1, 0).over(w_all_by_policy_premium).alias("next_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_id_adjuster_level).alias("rows_in_policy_id_adjuster_level"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_id), 2).alias("adjuster_level_vs_policy_id_avg"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.row_number().over(w_all_by_approved_amount).alias("seq_no"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_policy_id), F.lit(0)), 4).alias("policy_premium_share_of_policy_id"),
        F.sum("approved_amount").over(w_all_by_adjuster_level_runnin).alias("running_approved_amount"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.min("policy_premium").over(w_all_by_adjuster_level_runnin).alias("min_policy_premium_so_far"),
        F.first("claim_date").over(w_all_by_claimed_amount_full).alias("first_claim_date"),
        F.size(F.collect_set("adjuster_team").over(w_policy_id_adjuster_level)).alias("distinct_adjuster_team_in_policy_id_adjuster_lev"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("adjuster_level_rank") <= 2) & (F.col("adjuster_team") == "Travel"))
)

result = (
    first_rows
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "adjuster_team",
        "adjuster_level",
        "policy_product",
        "policy_premium",
        "approved_amount_vs_policy_id_adjuster_level_avg",
        "adjuster_level_rank",
        "rows_in_policy_id_adjuster_level",
        "adjuster_level_vs_policy_id_avg",
        "rows_in_policy_id",
        "policy_premium_share_of_policy_id",
        "running_approved_amount",
        "claimed_amount_cume_dist",
        "approved_amount_cume_dist",
        "first_claim_date",
        "distinct_adjuster_team_in_policy_id_adjuster_lev",
        (F.col("seq_no") + F.col("running_approved_amount")).alias("seq_no_plus_running_approved_amount"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
    )
    .orderBy("claimed_amount", "id")
    .limit(20)
)
