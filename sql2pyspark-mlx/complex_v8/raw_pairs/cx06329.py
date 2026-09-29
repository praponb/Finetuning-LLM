from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_approved_amount_movin = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)

base = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-02-23").cast("date"), F.lit("2025-05-31").cast("date")))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent_policy = (
    deduped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

combined_details_adjuster = (
    with_parent_policy.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.policy_start_date",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

metrics = (
    combined_details_adjuster
    .select(
        "*",
        F.size(F.collect_set("adjuster_team").over(w_adjuster_id)).alias("distinct_adjuster_team_in_adjuster_id"),
        F.round(F.avg("approved_amount").over(w_all_by_approved_amount_movin), 2).alias("approved_amount_avg_7"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "status",
        "policy_start_date",
        "adjuster_name",
        "adjuster_level",
        "approved_amount_avg_7",
        F.dayofweek(F.col("policy_start_date")).alias("policy_start_date_dayofweek"),
    )
    .orderBy("approved_amount_avg_7")
)
