from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_holder_region_by_r = Window.partitionBy("policy_id", "holder_region").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_holder_region_by_r_2 = Window.partitionBy("policy_id", "holder_region").orderBy(F.col("reported_at").desc(), "id")
w_all_by_policy_start_date = Window.orderBy(F.col("policy_start_date").desc(), "id")
w_holder_region_by_reported_at = Window.partitionBy("holder_region").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_by_reported_at_t = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc())
w_policy_id_status_by_policy_s = Window.partitionBy("policy_id", "status").orderBy("policy_start_date", "id")

filtered_claims = (
    df_claims
    .filter(F.col("approved_amount").between(0.0, 24718.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

joined = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.region").alias("holder_region"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.nth_value("reported_at", 2).over(w_policy_id_holder_region_by_r).alias("second_reported_at"),
        F.row_number().over(w_policy_id_holder_region_by_r_2).alias("reported_at_seq"),
        F.first("policy_start_date").over(w_policy_id_holder_region_by_r).alias("first_policy_start_date"),
        F.lag("policy_start_date").over(w_all_by_policy_start_date).alias("prev_policy_start_date"),
        F.row_number().over(w_holder_region_by_reported_at).alias("reported_at_seq_2"),
        F.round(F.cume_dist().over(w_adjuster_id_by_reported_at_t), 3).alias("reported_at_cume_dist"),
        F.dense_rank().over(w_policy_id_status_by_policy_s).alias("policy_start_date_dense_rank"),
        F.lead("reported_at").over(w_policy_id_status_by_policy_s).alias("next_reported_at"),
        F.row_number().over(w_all_by_policy_start_date).alias("policy_start_date_seq"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.least(F.col("reported_at_seq_2"), F.col("reported_at_seq")).alias("least_reported_at_seq_2_reported_at_seq"),
        (F.col("reported_at_seq_2") + F.col("policy_start_date_seq")).alias("reported_at_seq_2_plus_policy_start_date_seq"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_region")), " ", "_")).alias("holder_region_slug"),
    )
)

result = (
    derived
    .filter(F.to_date(F.col("next_reported_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "policy_start_date",
        "holder_region",
        "second_reported_at",
        "reported_at_seq",
        "first_policy_start_date",
        "reported_at_seq_2",
        "policy_start_date_dense_rank",
        "next_reported_at",
        "policy_start_date_seq",
        "least_reported_at_seq_2_reported_at_seq",
        "holder_region_slug",
    )
    .orderBy(F.col("policy_start_date_dense_rank").desc())
)
