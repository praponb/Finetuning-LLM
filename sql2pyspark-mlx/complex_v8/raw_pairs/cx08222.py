from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_all_by_reported_at_moving_2 = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_claims
    .filter(F.col("policy_id") <= 22)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.when(F.col("status").isin("PAID", "OPEN"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_trends = (
    valid
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "claim_type_status_label",
        F.count(F.lit(1)).over(w_adjuster_id_status).alias("rows_in_adjuster_id_status"),
        F.max("approved_amount").over(w_all_by_reported_at_moving_2).alias("approved_amount_max_3"),
    )
)

with_details_policy = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_status_label",
        "s.rows_in_adjuster_id_status",
        "s.approved_amount_max_3",
        F.col("p.premium").alias("policy_premium"),
        F.col("po.region").alias("holder_region"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

screened = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "claim_type_status_label",
        "rows_in_adjuster_id_status",
        "approved_amount_max_3",
        "holder_region",
        "holder_risk_score",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
    .orderBy(F.col("id").asc(), F.col("holder_region").desc_nulls_first())
)
