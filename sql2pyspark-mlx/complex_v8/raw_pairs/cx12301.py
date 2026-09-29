from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_distinct_status_by_di = Window.partitionBy("status", "distinct_status").orderBy(F.col("distinct_status").desc_nulls_first(), "status", "adjuster_id", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_distinct_status_by_di_2 = Window.partitionBy("status", "distinct_status").orderBy(F.col("distinct_status").desc_nulls_first())
w_adjuster_id = Window.partitionBy("adjuster_id")
w_n_rows_adjuster_id_by_n_stat = Window.partitionBy("n_rows", "adjuster_id").orderBy("n_status_paid")
w_adjuster_id_by_n_status_paid = Window.partitionBy("adjuster_id").orderBy("n_status_paid_vs_adjuster_id_avg")
w_adjuster_id_by_n_status_paid_2 = Window.partitionBy("adjuster_id").orderBy("n_status_paid_vs_adjuster_id_avg", "status", "adjuster_id", "policy_id")

scoped_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 43500.4) | (F.col("adjuster_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

derived = (
    scoped_claims
    .filter(F.col("claim_date").between(F.lit("2024-04-15").cast("date"), F.lit("2025-04-14").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        F.concat_ws("-", F.col("status"), F.col("claim_type_slug")).alias("status_claim_type_slug_label"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") > 3703.0), "left_anti")
)

per_group = (
    filtered
    .groupBy("status", "adjuster_id", "policy_id")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("n_status_paid"),
    )
    .filter(F.col("n_rows") >= 2)
)

windowed = (
    per_group
    .select(
        "*",
        F.last("n_rows").over(w_status_distinct_status_by_di).alias("last_n_rows"),
        F.dense_rank().over(w_status_distinct_status_by_di_2).alias("distinct_status_dense_rank"),
        F.round(F.col("n_status_paid") - F.avg("n_status_paid").over(w_adjuster_id), 2).alias("n_status_paid_vs_adjuster_id_avg"),
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_adjuster_id), F.lit(0)), 4).alias("n_rows_share_of_adjuster_id"),
        F.round(F.cume_dist().over(w_n_rows_adjuster_id_by_n_stat), 3).alias("n_status_paid_cume_dist"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.dense_rank().over(w_adjuster_id_by_n_status_paid).alias("n_status_paid_vs_adjuster_id_avg_dense_rank"),
        F.round(F.cume_dist().over(w_adjuster_id_by_n_status_paid_2), 3).alias("n_status_paid_vs_adjuster_id_avg_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "status",
        "adjuster_id",
        "n_status_paid_vs_adjuster_id_avg_dense_rank",
        "n_rows",
        "last_n_rows",
        "n_status_paid",
    )
    .unpivot(
        ids=["status", "adjuster_id"],
        values=["n_status_paid_vs_adjuster_id_avg_dense_rank", "n_rows", "last_n_rows", "n_status_paid"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
