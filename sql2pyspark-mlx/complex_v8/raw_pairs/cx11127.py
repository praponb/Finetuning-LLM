from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_team = Window.partitionBy("status", "adjuster_team")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date", "id")
w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_policy_id_by_clai_2 = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_policy_id_by_clai_3 = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date", "id").rowsBetween(-3, -1)
w_adjuster_id_policy_id_by_cla_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id")

filtered_claims = (
    df_claims
    .filter((F.col("status") == "PAID") & (F.col("adjuster_id") <= 7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

joined_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

metrics = (
    joined_adjuster
    .select(
        "*",
        F.last("claim_date", ignorenulls=True).over(w_adjuster_id_policy_id_by_cla).alias("last_known_claim_date"),
        F.size(F.collect_set("adjuster_id").over(w_status_adjuster_team)).alias("distinct_adjuster_id_in_status_adjuster_team"),
        F.count(F.lit(1)).over(w_status_adjuster_team).alias("rows_in_status_adjuster_team"),
        F.round(F.cume_dist().over(w_claim_type_policy_id_by_clai), 3).alias("claim_date_cume_dist"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_adjuster_team), 2).alias("approved_amount_vs_status_adjuster_team_avg"),
        F.sum("approved_amount").over(w_status_adjuster_team).alias("status_adjuster_team_sum_approved_amount"),
        F.sum("claimed_amount").over(w_all_by_claim_date_running).alias("running_claimed_amount"),
        F.last("approved_amount", ignorenulls=True).over(w_claim_type_policy_id_by_clai_2).alias("last_known_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_status_adjuster_team)).alias("distinct_policy_id_in_status_adjuster_team"),
        F.sum("claimed_amount").over(w_claim_type_policy_id_by_clai_3).alias("claimed_amount_sum_3_prior"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_adjuster_team), F.lit(0)), 4).alias("claimed_amount_share_of_status_adjuster_team"),
        F.row_number().over(w_adjuster_id_policy_id_by_cla_2).alias("claim_date_seq"),
    )
)

top_rows = metrics.filter(F.col("claim_date_seq") == 1)

rollup_base = (
    top_rows
    .groupBy("adjuster_id")
    .agg(
        F.sum("status_adjuster_team_sum_approved_amount").alias("total_status_adjuster_team_sum_approved_amount"),
        F.max("distinct_adjuster_id_in_status_adjuster_team").alias("max_distinct_adjuster_id_in_status_adjuster_team"),
        F.count("*").alias("n_rows"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.round(F.sum("last_known_approved_amount") / F.nullif(F.sum("status_adjuster_team_sum_approved_amount"), F.lit(0)), 3).alias("last_known_approved_amount_to_status_adjuster_te"),
        F.avg("claim_date_cume_dist").alias("avg_claim_date_cume_dist_check"),
    )
    .filter(F.col("avg_claim_date_cume_dist_check") > 0.3)
    .drop("avg_claim_date_cume_dist_check")
)

result = (
    rollup_base
    .filter(F.col("last_known_approved_amount_to_status_adjuster_te").between(1.0, 9.0))
    .select(
        "adjuster_id",
        "max_distinct_adjuster_id_in_status_adjuster_team",
        "n_rows",
        "distinct_adjuster_ids",
        "last_known_approved_amount_to_status_adjuster_te",
        F.when(F.col("n_rows") >= 10, "A").when(F.col("n_rows") >= 5, "B").otherwise("C").alias("n_rows_tier"),
        (F.floor(F.col("distinct_adjuster_ids") / 2) * 2).alias("distinct_adjuster_ids_bucket"),
    )
    .orderBy(F.col("distinct_adjuster_ids_bucket").desc(), "distinct_adjuster_ids")
)
