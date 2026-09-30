from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_status").orderBy(F.col("policy_premium").desc(), F.col("policy_id").desc(), F.col("status").desc())

base_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

grouped = (
    base_claims
    .groupBy("policy_id", "status")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
        F.count("*").alias("cnt"),
        F.avg("claim_type_len").alias("avg_claim_type_len_check"),
    )
    .filter(F.col("avg_claim_type_len_check") > 5)
    .drop("avg_claim_type_len_check")
)

enriched = (
    grouped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-05-30").cast("date")), "left_anti")
)

joined = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium_2"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

filtered = (
    joined.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("policy_id") >= 20)
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "policy_id",
        "status",
        "stddev_approved_amount",
        "cnt",
        "policy_premium",
        "policy_product",
        "policy_status",
        "policy_premium_2",
        "holder_risk_score",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy(F.col("status_slug").desc())
)
