from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_reported_at_month_by = Window.partitionBy("status", F.month("reported_at")).orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-6, -1)
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_all_by_policy_end_date = Window.orderBy("policy_end_date", "id")
w_adjuster_id_policy_end_date = Window.partitionBy("adjuster_id", F.month("policy_end_date"))
w_distinct_adjuster_id_in_adju = Window.partitionBy("distinct_adjuster_id_in_adjuster_level")
w_claimed_amount_tercile_adjus = Window.partitionBy("claimed_amount_tercile", "adjuster_level").orderBy(F.col("claimed_amount").desc(), "id")
w_distinct_adjuster_id_in_adju_2 = Window.partitionBy("distinct_adjuster_id_in_adjuster_level", "adjuster_level").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.status_clean",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_trends = (
    joined
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "policy_end_date",
        "adjuster_level",
        F.round(F.avg("claimed_amount").over(w_status_reported_at_month_by), 2).alias("claimed_amount_avg_6_prior"),
        F.dense_rank().over(w_adjuster_id_by_claimed_amoun).alias("claimed_amount_dense_rank"),
        F.ntile(3).over(w_adjuster_id_by_claimed_amoun_2).alias("claimed_amount_tercile"),
        F.size(F.collect_set("adjuster_id").over(w_adjuster_level)).alias("distinct_adjuster_id_in_adjuster_level"),
        F.lead("claimed_amount").over(w_all_by_policy_end_date).alias("next_claimed_amount"),
        F.min("adjuster_level").over(w_adjuster_id_policy_end_date).alias("adjuster_id_policy_end_date_month_min_adjuster_l"),
        F.sum("approved_amount").over(w_adjuster_id_policy_end_date).alias("adjuster_id_policy_end_date_month_sum_approved_a"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("claimed_amount_dense_rank") == 1)
    .drop("claimed_amount_dense_rank")
)

windowed = (
    first_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_adjuster_id_in_adju).alias("rows_in_distinct_adjuster_id_in_adjuster_level"),
        F.round(F.col("claimed_amount_avg_6_prior") / F.nullif(F.sum("claimed_amount_avg_6_prior").over(w_distinct_adjuster_id_in_adju), F.lit(0)), 4).alias("claimed_amount_avg_6_prior_share_of_distinct_adj"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claimed_amount_tercile_adjus)).alias("claimed_amount_change"),
        F.first("adjuster_level").over(w_distinct_adjuster_id_in_adju_2).alias("first_adjuster_level"),
    )
)

result = (
    windowed
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claimed_amount_avg_6_prior",
        "claimed_amount_tercile",
        "distinct_adjuster_id_in_adjuster_level",
        "adjuster_id_policy_end_date_month_min_adjuster_l",
        "adjuster_id_policy_end_date_month_sum_approved_a",
        "rows_in_distinct_adjuster_id_in_adjuster_level",
        "claimed_amount_avg_6_prior_share_of_distinct_adj",
        "claimed_amount_change",
        "first_adjuster_level",
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
    .orderBy("claimed_amount")
)
