from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_adjuster_id = Window.partitionBy("adjuster_id")
w_adjuster_team_policy_id_by_a = Window.partitionBy("adjuster_team", "policy_id").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-3, Window.currentRow)
w_adjuster_team_policy_id_by_a_2 = Window.partitionBy("adjuster_team", "policy_id").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_team_policy_id_by_a_3 = Window.partitionBy("adjuster_team", "policy_id").orderBy(F.col("adjuster_level").desc(), "id")
w_claim_type_group_by_claimed = Window.partitionBy("claim_type_group").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())

scoped = (
    df_claims
    .filter(F.col("claimed_amount") >= 7204.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("theft", "fire"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_group",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.avg("adjuster_level").over(w_claim_date_month), 2).alias("claim_date_month_avg_adjuster_level"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id"),
        F.max("claimed_amount").over(w_adjuster_team_policy_id_by_a).alias("claimed_amount_max_4"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.round(F.avg("adjuster_level").over(w_adjuster_team_policy_id_by_a_2), 2).alias("adjuster_level_avg_3"),
        F.ntile(10).over(w_adjuster_team_policy_id_by_a_3).alias("adjuster_level_decile"),
        F.min("adjuster_level").over(w_claim_type_group_by_claimed).alias("min_adjuster_level_so_far"),
        F.max("claimed_amount").over(w_claim_date_month).alias("claim_date_month_max_claimed_amount"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
    )
)

annotated = metrics.withColumn("claimed_amount_distance", F.abs(F.col("claimed_amount") - 29359.4))

result = (
    annotated
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "id",
        "policy_id",
        "status",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        "claimed_amount_share_of_adjuster_id",
        "distinct_policy_id_in_adjuster_id",
        "adjuster_level_avg_3",
        "adjuster_level_decile",
        "min_adjuster_level_so_far",
        "claim_date_month_max_claimed_amount",
        "claimed_amount_distance",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        (F.col("claim_date_cume_dist") - F.col("claimed_amount_distance")).alias("claim_date_cume_dist_minus_claimed_amount_distan"),
    )
    .orderBy("claim_date_label", "adjuster_team")
)
