from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status_by_policy_end = Window.partitionBy("policy_status").orderBy("policy_end_date")
w_policy_id_claim_type_by_adju = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("adjuster_level").desc(), "id")
w_policy_id_claim_type_by_poli = Window.partitionBy("policy_id", "claim_type").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_status = Window.partitionBy("policy_status")
w_policy_status_policy_id_by_p = Window.partitionBy("policy_status", "policy_id").orderBy(F.col("policy_end_date").desc(), "id")
w_policy_status_policy_id_by_p_2 = Window.partitionBy("policy_status", "policy_id").orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type_by_adju_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-6, -1)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_adjuster_level_runnin = Window.orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_adjuster_level = Window.partitionBy("claim_type").orderBy(F.col("adjuster_level_percent_rank").desc(), "id")

cleaned_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(8, 11)) & (F.col("approved_amount") > 4868.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
    )
)

combined_details_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Travel"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

metrics = (
    combined_details_adjuster
    .select(
        "*",
        F.dense_rank().over(w_policy_status_by_policy_end).alias("policy_end_date_dense_rank"),
        F.round(F.percent_rank().over(w_policy_id_claim_type_by_adju), 3).alias("adjuster_level_percent_rank"),
        F.round(F.percent_rank().over(w_policy_status_by_policy_end), 3).alias("policy_end_date_percent_rank"),
        F.min("adjuster_level").over(w_policy_id_claim_type_by_poli).alias("min_adjuster_level_so_far"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_status), F.lit(0)), 4).alias("adjuster_level_share_of_policy_status"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_policy_status_policy_id_by_p)).alias("adjuster_level_change"),
        F.sum("adjuster_level").over(w_policy_status_policy_id_by_p_2).alias("running_adjuster_level"),
        F.min("adjuster_level").over(w_policy_status).alias("policy_status_min_adjuster_level"),
        F.max("adjuster_level").over(w_policy_id_claim_type_by_adju_2).alias("adjuster_level_max_6_prior"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_id), 2).alias("adjuster_level_vs_policy_id_avg"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.first("running_adjuster_level", ignorenulls=True).over(w_all_by_adjuster_level_runnin).alias("first_known_running_adjuster_level"),
        F.ntile(4).over(w_claim_type_by_adjuster_level).alias("adjuster_level_percent_rank_quartile"),
        F.round(F.col("adjuster_level_percent_rank") - F.avg("adjuster_level_percent_rank").over(w_policy_status), 2).alias("adjuster_level_percent_rank_vs_policy_status_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "adjuster_level",
        "policy_end_date",
        "policy_end_date_dense_rank",
        "adjuster_level_percent_rank",
        "adjuster_level_share_of_policy_status",
        "adjuster_level_change",
        "first_known_running_adjuster_level",
        "adjuster_level_percent_rank_quartile",
        "adjuster_level_percent_rank_vs_policy_status_avg",
        F.substring(F.trim(F.col("policy_status")), 1, 2).alias("policy_status_prefix"),
    )
    .orderBy(F.col("policy_status_prefix").desc_nulls_first(), "id")
    .limit(10)
)
