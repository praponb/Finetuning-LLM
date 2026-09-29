from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_adjuster_id_claim_type_by_cl = Window.partitionBy("adjuster_id", "claim_type").orderBy("claim_date_week", "id").rowsBetween(-6, -1)
w_adjuster_id_claim_type_by_cl_2 = Window.partitionBy("adjuster_id", "claim_type").orderBy("claim_date_week", "id").rowsBetween(-6, Window.currentRow)
w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date_mo = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id_by_claim_date_fu = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_claim_date_week_month = Window.partitionBy("status", F.month("claim_date_week"))
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id")

prepared = (
    df_claims
    .filter(F.col("claimed_amount").between(17759.3, 29985.9))
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
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_adjuster_id_status)).alias("distinct_policy_id_in_adjuster_id_status"),
        F.max("approved_amount").over(w_adjuster_id_claim_type_by_cl).alias("approved_amount_max_6_prior"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_claim_type_by_cl_2), 2).alias("approved_amount_avg_7"),
        F.min("approved_amount").over(w_adjuster_id_status).alias("adjuster_id_status_min_approved_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_status), 2).alias("claimed_amount_vs_adjuster_id_status_avg"),
        F.last("approved_amount", ignorenulls=True).over(w_all_by_claim_date_running).alias("last_known_approved_amount"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.max("approved_amount_max_6_prior").over(w_adjuster_id_by_claim_date_mo).alias("approved_amount_max_6_prior_max_3"),
        F.nth_value("approved_amount_max_6_prior", 2).over(w_adjuster_id_by_claim_date_fu).alias("second_approved_amount_max_6_prior"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_claim_date_week_month), 2).alias("approved_amount_vs_status_claim_date_week_month_"),
        F.round(F.col("approved_amount_avg_7") / F.nullif(F.sum("approved_amount_avg_7").over(w_status_claim_date_week_month), F.lit(0)), 4).alias("approved_amount_avg_7_share_of_status_claim_date"),
        (F.col("approved_amount_max_6_prior") - F.lag("approved_amount_max_6_prior").over(w_adjuster_id_by_claim_date)).alias("approved_amount_max_6_prior_change"),
    )
)

totals = (
    with_windows
    .filter(F.col("approved_amount_avg_7_share_of_status_claim_date") >= 0.1)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status").isin("REJECTED", "OPEN"), F.col("approved_amount"))).alias("approved_amount_status_in_rejected_open"),
        F.round(100 * F.count(F.when(F.col("status").isin("OPEN", "REJECTED", "PAID"), 1)) / F.count("*"), 1).alias("pct_status_in_open_rejected_paid"),
        F.avg("second_approved_amount_max_6_prior").alias("avg_second_approved_amount_max_6_prior_check"),
    )
    .filter(F.col("avg_second_approved_amount_max_6_prior_check") > 22500.0)
    .drop("avg_second_approved_amount_max_6_prior_check")
)

result = (
    totals
    .select(
        "status",
        "pct_status_in_open_rejected_paid",
        "approved_amount_status_in_rejected_open",
    )
    .unpivot(
        ids=["status"],
        values=["pct_status_in_open_rejected_paid", "approved_amount_status_in_rejected_open"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
