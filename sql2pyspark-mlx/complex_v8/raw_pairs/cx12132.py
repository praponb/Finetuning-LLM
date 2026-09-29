from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_adjuster_level = Window.partitionBy("policy_id").orderBy("adjuster_level")
w_policy_id_by_adjuster_level_2 = Window.partitionBy("policy_id").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_policy_id_by_adjuster_level_3 = Window.partitionBy("policy_id").orderBy("adjuster_level", "id")
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_status_by_claimed_amount_pri = Window.partitionBy("status").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_claim_type = Window.partitionBy("claim_type")
w_policy_id_by_adjuster_level_4 = Window.partitionBy("policy_id").orderBy(F.col("adjuster_level_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_adjuster_level_5 = Window.partitionBy("policy_id").orderBy(F.col("adjuster_level_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_tercile_r = Window.orderBy("reported_at_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_reported_at_tercile = Window.orderBy("reported_at_tercile", "id")

cleaned = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("adjuster_id").isNotNull()))
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
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

windowed = (
    with_parent
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_id_by_adjuster_level), 3).alias("adjuster_level_cume_dist"),
        F.count(F.lit(1)).over(w_policy_id_by_adjuster_level_2).alias("running_count"),
        F.dense_rank().over(w_policy_id_by_approved_amount).alias("approved_amount_dense_rank"),
        F.row_number().over(w_policy_id_by_adjuster_level_3).alias("seq_no"),
        F.size(F.collect_set("status").over(w_claim_date_month)).alias("distinct_status_in_claim_date_month"),
        F.ntile(3).over(w_all_by_reported_at).alias("reported_at_tercile"),
        F.round(F.avg("approved_amount").over(w_status_by_claimed_amount_pri), 2).alias("approved_amount_avg_2_prior"),
        F.max("approved_amount").over(w_claim_type).alias("claim_type_max_approved_amount"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.nth_value("adjuster_level_cume_dist", 2).over(w_policy_id_by_adjuster_level_4).alias("second_adjuster_level_cume_dist"),
        F.first("approved_amount", ignorenulls=True).over(w_policy_id_by_adjuster_level_5).alias("first_known_approved_amount"),
        F.last("claimed_amount", ignorenulls=True).over(w_all_by_reported_at_tercile_r).alias("last_known_claimed_amount"),
        F.round(F.col("reported_at_tercile") - F.avg("reported_at_tercile").over(w_policy_id), 2).alias("reported_at_tercile_vs_policy_id_avg"),
        F.rank().over(w_all_by_reported_at_tercile).alias("reported_at_tercile_rank"),
        F.last("adjuster_level", ignorenulls=True).over(w_all_by_reported_at_tercile_r).alias("last_known_adjuster_level"),
    )
)

result = (
    metrics
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
        "claim_type_ref",
        "adjuster_level",
        "running_count",
        "approved_amount_dense_rank",
        "seq_no",
        "distinct_status_in_claim_date_month",
        "reported_at_tercile",
        "approved_amount_avg_2_prior",
        "claim_type_max_approved_amount",
        "second_adjuster_level_cume_dist",
        "last_known_claimed_amount",
        "last_known_adjuster_level",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
    .orderBy(F.col("approved_amount").desc(), F.col("claimed_amount").desc(), "id")
    .limit(10)
)
