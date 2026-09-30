from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_policy_start_date_mov = Window.orderBy("policy_start_date", "id").rowsBetween(-6, Window.currentRow)
w_policy_start_date_year_adjus = Window.partitionBy(F.year("policy_start_date"), "adjuster_id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_adjuster_id_policy_status = Window.partitionBy("adjuster_id", "policy_status")
w_policy_holder_id_status_by_p = Window.partitionBy("policy_holder_id", "status").orderBy("policy_start_date")
w_all_by_policy_start_date = Window.orderBy("policy_start_date", "id")

cleaned = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_per_approved_amount",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("po.risk_score").alias("holder_risk_score"),
        "po.holder_name",
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.sum("approved_amount").over(w_all_by_policy_start_date_mov).alias("approved_amount_sum_7"),
        F.min("approved_amount").over(w_policy_start_date_year_adjus).alias("policy_start_date_year_adjuster_id_min_approved_"),
        F.size(F.collect_set("policy_status").over(w_adjuster_id)).alias("distinct_policy_status_in_adjuster_id"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_status).alias("rows_in_adjuster_id_policy_status"),
        F.count(F.lit(1)).over(w_policy_start_date_year_adjus).alias("rows_in_policy_start_date_year_adjuster_id"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_adjuster_id_policy_status), F.lit(0)), 4).alias("holder_risk_score_share_of_adjuster_id_policy_st"),
        F.rank().over(w_policy_holder_id_status_by_p).alias("policy_start_date_rank"),
        F.sum("holder_risk_score").over(w_policy_start_date_year_adjus).alias("policy_start_date_year_adjuster_id_sum_holder_ri"),
        F.max("holder_risk_score").over(w_adjuster_id).alias("adjuster_id_max_holder_risk_score"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_status), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_stat"),
        F.lead("policy_start_date", 3).over(w_all_by_policy_start_date).alias("next3_policy_start_date"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_adjuster_id_policy_status), 2).alias("holder_risk_score_vs_adjuster_id_policy_status_a"),
        F.round(F.avg("approved_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_approved_amount"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "claimed_amount_per_approved_amount",
        "policy_holder_id",
        "policy_start_date",
        "policy_status",
        "policy_start_date_year_adjuster_id_min_approved_",
        "rows_in_adjuster_id_policy_status",
        "rows_in_policy_start_date_year_adjuster_id",
        "policy_start_date_rank",
        "policy_start_date_year_adjuster_id_sum_holder_ri",
        "adjuster_id_max_holder_risk_score",
        "approved_amount_share_of_adjuster_id_policy_stat",
        "next3_policy_start_date",
        "holder_risk_score_vs_adjuster_id_policy_status_a",
        "adjuster_id_avg_approved_amount",
    )
    .orderBy(
        F.col("holder_risk_score_vs_adjuster_id_policy_status_a").desc(),
        F.col("policy_start_date_year_adjuster_id_sum_holder_ri").asc(),
    )
)
