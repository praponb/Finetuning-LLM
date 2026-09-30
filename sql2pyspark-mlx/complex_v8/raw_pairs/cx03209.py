from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id")
w_adjuster_id_policy_id_by_cla_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id_by_cla_3 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date")
w_all_by_total_running_count_p = Window.orderBy("total_running_count", "prev_reported_at_month", "prev2_claim_date_year", "claim_date_year").rowsBetween(-6, -1)
w_distinct_ids_by_first_prev2 = Window.partitionBy("distinct_ids").orderBy("first_prev2_claim_date", "prev_reported_at_month", "prev2_claim_date_year", "claim_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean_2"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.lag("reported_at").over(w_adjuster_id_policy_id_by_cla).alias("prev_reported_at"),
        F.sum("claimed_amount").over(w_claim_date_month_adjuster_id).alias("claim_date_month_adjuster_id_sum_claimed_amount"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_id_by_cla_2).alias("running_count"),
        F.lag("claim_date", 2).over(w_adjuster_id_policy_id_by_cla).alias("prev2_claim_date"),
        F.rank().over(w_adjuster_id_policy_id_by_cla).alias("claim_date_rank"),
        F.rank().over(w_adjuster_id_policy_id_by_cla_3).alias("claim_date_rank_2"),
    )
)

stats = (
    ranked
    .groupBy(
        F.date_trunc("month", "prev_reported_at").alias("prev_reported_at_month"),
        F.year("prev2_claim_date").alias("prev2_claim_date_year"),
        F.year("claim_date").alias("claim_date_year"),
    )
    .agg(
        F.round(F.sum("claim_date_month_adjuster_id_sum_claimed_amount") / F.nullif(F.sum("claim_date_rank_2"), F.lit(0)), 3).alias("claim_date_month_adjuster_id_sum_claimed_amount_"),
        F.min("prev2_claim_date").alias("first_prev2_claim_date"),
        F.percentile_approx("claim_date_month_adjuster_id_sum_claimed_amount", 0.5).alias("median_claim_date_month_adjuster_id_sum_claimed_"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("running_count").alias("total_running_count"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.sum("claim_date_rank_2").alias("sum_claim_date_rank_2_check"),
    )
    .filter(F.col("sum_claim_date_rank_2_check") > 2)
    .drop("sum_claim_date_rank_2_check")
)

sequenced = (
    stats
    .select(
        "prev_reported_at_month",
        "prev2_claim_date_year",
        "claim_date_year",
        "claim_date_month_adjuster_id_sum_claimed_amount_",
        "total_running_count",
        "avg_claimed_amount",
        F.max("distinct_ids").over(w_all_by_total_running_count_p).alias("distinct_ids_max_6_prior"),
        F.count(F.lit(1)).over(w_distinct_ids_by_first_prev2).alias("running_count"),
    )
)

filtered = (
    sequenced
    .crossJoin(sequenced.agg(F.min("claim_date_month_adjuster_id_sum_claimed_amount_").alias("overall_min_claim_date_month_adjuster_id_sum_cla")))
    .filter(F.col("claim_date_month_adjuster_id_sum_claimed_amount_") > F.col("overall_min_claim_date_month_adjuster_id_sum_cla"))
    .drop("overall_min_claim_date_month_adjuster_id_sum_cla")
)

result = (
    filtered
    .select(
        "prev_reported_at_month",
        "prev2_claim_date_year",
        F.col("avg_claimed_amount").alias("avg_claimed_amount_v"),
        F.col("claim_date_month_adjuster_id_sum_claimed_amount_").alias("m_claim_date_month_adjuster_id_sum_claimed_amoun"),
    )
    .unpivot(
        ids=["prev_reported_at_month", "prev2_claim_date_year"],
        values=["avg_claimed_amount_v", "m_claim_date_month_adjuster_id_sum_claimed_amoun"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
