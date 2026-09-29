from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy("reported_at", "id")
w_policy_id_reported_at_to_dat = Window.partitionBy("policy_id", F.to_date("reported_at")).orderBy("reported_at", "id")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

recent_claims = (
    df_claims
    .filter(F.col("approved_amount") <= 14993.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.concat_ws("-", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

metrics = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_or_unknown",
        F.round(F.avg("claimed_amount").over(w_adjuster_id_by_claimed_amoun), 2).alias("claimed_amount_avg_3"),
        F.lead("claimed_amount", 3, 0).over(w_adjuster_id_by_reported_at).alias("next3_claimed_amount"),
        F.lead("reported_at").over(w_policy_id_reported_at_to_dat).alias("next_reported_at"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_adjuster_id_by_claimed_amoun_2)).alias("claimed_amount_change"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_claimed_amount"),
    )
)

result = (
    metrics
    .rollup("policy_id", "adjuster_id", "status")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
