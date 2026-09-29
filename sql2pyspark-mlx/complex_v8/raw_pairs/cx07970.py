from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").asc_nulls_last())
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")

base_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 2)) & (F.col("claim_date").between(F.lit("2024-02-17").cast("date"), F.lit("2025-04-14").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.when(F.col("claim_type").isin("theft", "medical"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

sequenced = (
    base_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "claim_type_prefix",
        "claim_type_group",
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_by_claimed_amoun), 2).alias("claimed_amount_avg_6_prior"),
        F.round(F.percent_rank().over(w_adjuster_id_by_approved_amou), 3).alias("approved_amount_percent_rank"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("claimed_amount_vs_adjuster_id_policy_id_avg"),
        F.lag("claimed_amount").over(w_all_by_approved_amount).alias("prev_claimed_amount"),
        F.lag("claimed_amount", 3).over(w_all_by_approved_amount).alias("prev3_claimed_amount"),
    )
)

claim_type_group_stats = (
    sequenced
    .groupBy("claim_type_group")
    .agg(F.avg("claimed_amount_avg_6_prior").alias("avg_claimed_amount_avg_6_prior"))
)

compared = (
    sequenced.alias("s")
    .join(claim_type_group_stats.alias("g"), F.col("g.claim_type_group") == F.col("s.claim_type_group"), "inner")
    .filter(F.col("s.claimed_amount_avg_6_prior") > F.col("g.avg_claimed_amount_avg_6_prior"))
    .select(
        "s.*",
        "g.avg_claimed_amount_avg_6_prior",
        F.round(F.col("s.claimed_amount_avg_6_prior") - F.col("g.avg_claimed_amount_avg_6_prior"), 2).alias("claimed_amount_avg_6_prior_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "claimed_amount_avg_6_prior_minus_avg",
        "prev3_claimed_amount",
        "claimed_amount_vs_adjuster_id_policy_id_avg",
        "avg_claimed_amount_avg_6_prior",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_avg_6_prior_minus_avg", "prev3_claimed_amount", "claimed_amount_vs_adjuster_id_policy_id_avg", "avg_claimed_amount_avg_6_prior"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
