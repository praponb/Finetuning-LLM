from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_reported_at_quarte = Window.partitionBy("policy_id", "reported_at_quarter_2")
w_reported_at_quarter_2_by_cla = Window.partitionBy("reported_at_quarter_2").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_quarter_2_by_cla_2 = Window.partitionBy("reported_at_quarter_2").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)

base_claims = (
    df_claims
    .filter(F.col("claimed_amount").between(13635.1, 31752.9))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter_2"),
    )
)

metrics = (
    base_claims
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at_quarter",
        "reported_at_quarter_2",
        F.round(F.avg("claimed_amount").over(w_policy_id_reported_at_quarte), 2).alias("policy_id_reported_at_quarter_2_avg_claimed_amou"),
        F.min("claimed_amount").over(w_reported_at_quarter_2_by_cla).alias("min_claimed_amount_so_far"),
        F.max("claimed_amount").over(w_policy_id_reported_at_quarte).alias("policy_id_reported_at_quarter_2_max_claimed_amou"),
        F.round(F.avg("reported_at_quarter_2").over(w_reported_at_quarter_2_by_cla_2), 2).alias("reported_at_quarter_2_avg_7"),
    )
)

combined_details_policy = (
    metrics.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    combined_details_policy
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at_quarter",
        "policy_id_reported_at_quarter_2_avg_claimed_amou",
        "min_claimed_amount_so_far",
        "policy_id_reported_at_quarter_2_max_claimed_amou",
        "reported_at_quarter_2_avg_7",
        "policy_premium",
        "policy_status",
        "holder_region",
        F.when(F.col("reported_at_quarter") >= 3, "HIGH").when(F.col("reported_at_quarter") >= 2, "MEDIUM").otherwise("LOW").alias("reported_at_quarter_tier"),
    )
)
