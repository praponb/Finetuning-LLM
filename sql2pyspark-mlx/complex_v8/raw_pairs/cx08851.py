from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy("end_date", "id")
w_status_holder_id_by_end_date_2 = Window.partitionBy("status", "holder_id").orderBy("end_date", "id").rowsBetween(-1, Window.currentRow)

base_policys = (
    df_policies
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_activity = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        (F.col("premium") - F.lag("premium").over(w_status_holder_id_by_end_date)).alias("premium_change"),
        F.max("premium").over(w_status_holder_id_by_end_date_2).alias("premium_max_2"),
    )
)

joined_holder = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region") == "Central")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.premium_change",
        "s.premium_max_2",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("delay", "theft", "medical"), 1)).alias("claim_claim_type_count"),
    )
)

with_counts = (
    joined_holder.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "premium_change",
        "premium_max_2",
        "claim_claim_type_count",
        F.least(F.col("premium_max_2"), F.col("premium_change")).alias("least_premium_max_2_premium_change"),
        F.abs(F.col("premium_change") - 2000.0).alias("premium_change_distance"),
    )
    .orderBy(F.col("least_premium_max_2_premium_change").desc_nulls_first())
)
