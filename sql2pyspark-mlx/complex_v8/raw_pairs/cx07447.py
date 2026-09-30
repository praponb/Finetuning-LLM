from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_policy_id = Window.partitionBy("status", "policy_id")
w_status_by_claim_date_running = Window.partitionBy("status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_month_status_by = Window.partitionBy(F.date_trunc("month", "reported_at"), "status").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_by_first_known_r = Window.partitionBy("adjuster_id").orderBy(F.col("first_known_reported_at").desc(), "id")
w_adjuster_id_by_first_known_r_2 = Window.partitionBy("adjuster_id").orderBy(F.col("first_known_reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_claims = (
    df_claims
    .filter(F.col("policy_id") <= 8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
    )
)

windowed = (
    filtered_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        F.min("approved_amount").over(w_status_policy_id).alias("status_policy_id_min_approved_amount"),
        F.first("reported_at", ignorenulls=True).over(w_status_by_claim_date_running).alias("first_known_reported_at"),
        F.round(F.cume_dist().over(w_reported_at_month_status_by), 3).alias("approved_amount_cume_dist"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.dense_rank().over(w_adjuster_id_by_first_known_r).alias("first_known_reported_at_dense_rank"),
        F.nth_value("approved_amount_cume_dist", 2).over(w_adjuster_id_by_first_known_r_2).alias("second_approved_amount_cume_dist"),
    )
)

result = (
    sequenced
    .rollup("status", "adjuster_id", "first_known_reported_at_dense_rank")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("first_known_reported_at_dense_rank").asc_nulls_last(),
    )
)
