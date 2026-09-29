from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc())
w_policy_id_claim_date_month_b_2 = Window.partitionBy("policy_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc(), "id")
w_policy_id_claim_date_month_b_3 = Window.partitionBy("policy_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_month_b_4 = Window.partitionBy("policy_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) | (F.col("adjuster_id") <= 4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.dense_rank().over(w_policy_id_claim_date_month_b).alias("claimed_amount_dense_rank"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_id_claim_date_month_b_2)).alias("approved_amount_change"),
        F.first("approved_amount", ignorenulls=True).over(w_policy_id_claim_date_month_b_3).alias("first_known_approved_amount"),
        F.round(F.avg("approved_amount").over(w_policy_id_claim_date_month_b_4), 2).alias("approved_amount_avg_4"),
    )
)

labeled = (
    metrics
    .filter(F.col("policy_id").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

screened = (
    labeled.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

stats = (
    screened
    .groupBy(F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"))
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") != "REJECTED", 1)).alias("n_status_rejected"),
        F.round(F.avg("rows_in_adjuster_id_policy_id"), 2).alias("avg_rows_in_adjuster_id_policy_id"),
        F.count("*").alias("n_rows"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

flattened = (
    stats
    .select(
        "claim_date_ym",
        "n_status_rejected",
        "avg_rows_in_adjuster_id_policy_id",
        "n_rows",
        "distinct_status",
        F.explode_outer("status_list").alias("status_list_item"),
    )
)

result = (
    flattened
    .select(
        "claim_date_ym",
        "n_status_rejected",
        "avg_rows_in_adjuster_id_policy_id",
        "n_rows",
        "distinct_status",
        "status_list_item",
    )
    .orderBy(F.col("n_status_rejected").asc_nulls_last())
)
