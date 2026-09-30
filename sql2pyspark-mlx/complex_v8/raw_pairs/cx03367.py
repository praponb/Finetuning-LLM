from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_claim_type_theft = Window.partitionBy("n_claim_type_theft")

base = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 15)) & (F.col("approved_amount") < 25402.3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

enriched = (
    base
    .filter(F.col("reported_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

per_group = (
    enriched
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        F.date_format("reported_at", "yyyy-MM").alias("reported_at_ym"),
        "status",
    )
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.count(F.when(F.col("claim_type") != "theft", 1)).alias("n_claim_type_theft"),
        F.min("approved_amount").alias("min_approved_amount"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.size(F.collect_set("status").over(w_n_claim_type_theft)).alias("distinct_status_in_n_claim_type_theft"),
        F.min("n_claim_type_theft").over(w_n_claim_type_theft).alias("n_claim_type_theft_min_n_claim_type_theft"),
        F.max("n_claim_type_theft").over(w_n_claim_type_theft).alias("n_claim_type_theft_max_n_claim_type_theft"),
        F.round(F.col("n_claim_type_theft") - F.avg("n_claim_type_theft").over(w_n_claim_type_theft), 2).alias("n_claim_type_theft_vs_n_claim_type_theft_avg"),
    )
)

result = (
    with_windows
    .select(
        "claim_date_month",
        "reported_at_ym",
        F.col("n_claim_type_theft_vs_n_claim_type_theft_avg").alias("n_claim_type_theft_vs_n_claim_type_theft_avg_v"),
        F.col("min_approved_amount").alias("m_min_approved_amount"),
    )
    .unpivot(
        ids=["claim_date_month", "reported_at_ym"],
        values=["n_claim_type_theft_vs_n_claim_type_theft_avg_v", "m_min_approved_amount"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("claim_date_month", "reported_at_ym", "metric")
)
