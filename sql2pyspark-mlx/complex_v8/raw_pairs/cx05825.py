from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_month_claim_type = Window.partitionBy(F.month("reported_at"), "claim_type").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-1, Window.currentRow)
w_status = Window.partitionBy("status")
w_reported_at_month_claim_type_2 = Window.partitionBy(F.month("reported_at"), "claim_type").orderBy(F.col("claimed_amount").desc())
w_policy_id = Window.partitionBy("policy_id")
w_status_reported_at_to_date = Window.partitionBy("status", F.to_date("reported_at"))
w_reported_at_month_claim_type_3 = Window.partitionBy(F.month("reported_at"), "claim_type").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_policy_id_status_by_reported = Window.partitionBy("policy_id", "status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

ranked = (
    prepared_claims
    .select(
        "*",
        F.first("claim_date", ignorenulls=True).over(w_claim_type_by_claimed_amount).alias("first_known_claim_date"),
        F.round(F.avg("claimed_amount").over(w_reported_at_month_claim_type), 2).alias("claimed_amount_avg_2"),
        F.max("claimed_amount").over(w_status).alias("status_max_claimed_amount"),
        F.round(F.percent_rank().over(w_reported_at_month_claim_type_2), 3).alias("claimed_amount_percent_rank"),
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_reported_at_to_date), 2).alias("claimed_amount_vs_status_reported_at_to_date_avg"),
        F.round(F.avg("claimed_amount").over(w_status_reported_at_to_date), 2).alias("status_reported_at_to_date_avg_claimed_amount"),
        F.first("claimed_amount", ignorenulls=True).over(w_reported_at_month_claim_type_3).alias("first_known_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claim_date)).alias("claimed_amount_change"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.first("claim_date", ignorenulls=True).over(w_reported_at_month_claim_type_3).alias("first_known_claim_date_2"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.min("claimed_amount").over(w_policy_id_status_by_reported).alias("min_claimed_amount_so_far"),
    )
)

annotated = (
    ranked
    .filter(F.col("claimed_amount_vs_status_reported_at_to_date_avg") <= 10000.0)
    .select(
        "*",
        F.date_format(F.col("first_known_claim_date"), "MMM yyyy").alias("first_known_claim_date_label"),
    )
)

summary = (
    annotated
    .filter(F.year(F.col("first_known_claim_date")) == 2024)
    .groupBy("adjuster_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.round(F.avg("policy_id_max_claimed_amount"), 2).alias("avg_policy_id_max_claimed_amount"),
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.avg("status_reported_at_to_date_avg_claimed_amount").alias("avg_status_reported_at_to_date_avg_claimed_amoun"),
    )
    .filter(F.col("avg_status_reported_at_to_date_avg_claimed_amoun") > 15070.0)
    .drop("avg_status_reported_at_to_date_avg_claimed_amoun")
)

result = (
    summary
    .select(
        "adjuster_id",
        "avg_claimed_amount",
        "stddev_claimed_amount",
        F.when(F.col("stddev_claimed_amount") >= 30000.0, "A").when(F.col("stddev_claimed_amount") >= 20000.0, "B").otherwise("C").alias("stddev_claimed_amount_tier"),
        F.coalesce(F.col("avg_claimed_amount"), F.lit(0.0)).alias("avg_claimed_amount_filled"),
    )
)
