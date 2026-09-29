from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_plus_12m_m = Window.orderBy(F.col("claim_date_plus_12m").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_claim_date_plus_12m = Window.orderBy(F.col("claim_date_plus_12m").desc(), "id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")

scoped = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.add_months(F.col("claim_date"), 12).alias("claim_date_plus_12m"),
        F.when(F.col("claim_type").isin("collision", "theft"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_all_by_claim_date_plus_12m_m), 2).alias("claimed_amount_avg_7"),
        F.lag("claim_date_plus_12m").over(w_all_by_claim_date_plus_12m).alias("prev_claim_date_plus_12m"),
        F.datediff(F.col("claim_date_plus_12m"), F.lag("claim_date_plus_12m").over(w_all_by_claim_date)).alias("days_since_prev_claim_date_plus_12m"),
        F.row_number().over(w_all_by_claim_date_plus_12m).alias("seq_no"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isNotNull()), "left_anti")
)

joined = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

first_rows = (
    joined
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

with_children = (
    first_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.policy_holder_id")) & (F.col("p.premium") >= 1042.5), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.claim_date_plus_12m",
        "s.days_since_prev_claim_date_plus_12m",
        "s.policy_holder_id",
        F.col("p.id").alias("policy_id_2"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date_2"),
        F.col("p.product").alias("policy_product"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status") != "REJECTED", 1)).alias("claim_status_count"),
    )
)

profiled = (
    with_children.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "policy_id",
        "claim_type",
        "status",
        "claim_date_plus_12m",
        "policy_holder_id",
        "policy_id_2",
        "policy_end_date_2",
        "policy_product",
        "claim_count",
        "claim_status_count",
    )
    .orderBy("claim_status_count")
)
