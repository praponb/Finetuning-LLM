from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_claim_type_status_by_approve = Window.partitionBy("claim_type", "status").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_claim_type_claim_date_plus_1 = Window.partitionBy("claim_type", F.month("claim_date_plus_1m")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_status_by_claim_da = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_claim_date_plus_1_2 = Window.partitionBy("claim_type", F.month("claim_date_plus_1m")).orderBy("approved_amount", "id")
w_claim_type_claim_date_plus_1_3 = Window.partitionBy("claim_type", F.month("claim_date_plus_1m")).orderBy("approved_amount", "id").rowsBetween(-6, -1)
w_policy_id_status_by_claim_da_2 = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").desc_nulls_first())
w_row_count_claim_date_month_m = Window.partitionBy("row_count", F.date_trunc("month", "claim_date_month"))
w_row_count_by_row_count_runni = Window.partitionBy("row_count").orderBy("row_count", "claim_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.add_months(F.col("claim_date"), 1).alias("claim_date_plus_1m"),
    )
)

with_windows = (
    recent_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.row_number().over(w_claim_type_status_by_approve).alias("approved_amount_seq"),
        F.count(F.lit(1)).over(w_claim_type_claim_date_plus_1).alias("running_count"),
        F.last("claimed_amount", ignorenulls=True).over(w_claim_type_claim_date_plus_1).alias("last_known_claimed_amount"),
        F.max("approved_amount").over(w_claim_type).alias("claim_type_max_approved_amount"),
        F.last("claim_date_plus_1m", ignorenulls=True).over(w_policy_id_status_by_claim_da).alias("last_known_claim_date_plus_1m"),
        F.row_number().over(w_claim_type_claim_date_plus_1_2).alias("approved_amount_seq_2"),
        F.sum("approved_amount").over(w_claim_type_claim_date_plus_1_3).alias("approved_amount_sum_6_prior"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.min("claimed_amount").over(w_claim_type).alias("claim_type_min_claimed_amount"),
        F.round(F.cume_dist().over(w_policy_id_status_by_claim_da_2), 3).alias("claim_date_cume_dist"),
        F.round(F.avg("approved_amount").over(w_claim_type), 2).alias("claim_type_avg_approved_amount"),
        F.lead("approved_amount").over(w_claim_type_claim_date_plus_1_2).alias("next_approved_amount"),
        F.min("approved_amount").over(w_claim_type).alias("claim_type_min_approved_amount"),
        F.round(F.percent_rank().over(w_claim_type_status_by_approve), 3).alias("approved_amount_percent_rank"),
    )
)

top_per_group = with_windows.filter(F.col("approved_amount_seq") <= 2)

summary = (
    top_per_group
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.sum("approved_amount_seq") / F.nullif(F.sum("rows_in_claim_type"), F.lit(0)), 3).alias("approved_amount_seq_to_rows_in_claim_type_ratio"),
        F.round(100 * F.count(F.when(F.col("claim_type") == "collision", 1)) / F.count("*"), 1).alias("pct_claim_type_collision"),
    )
    .filter(F.col("row_count") >= 3)
)

metrics = (
    summary
    .withColumn("rows_in_row_count_claim_date_month_month", F.count(F.lit(1)).over(w_row_count_claim_date_month_m))
    .withColumn("last_known_row_count", F.last("row_count", ignorenulls=True).over(w_row_count_by_row_count_runni))
)

result = (
    metrics
    .select(
        "claim_date_month",
        "row_count",
        "claim_type_list",
        "approved_amount_seq_to_rows_in_claim_type_ratio",
        "pct_claim_type_collision",
        "rows_in_row_count_claim_date_month_month",
        "last_known_row_count",
        F.month(F.col("claim_date_month")).alias("claim_date_month_month"),
        F.when(F.col("last_known_row_count") >= 10, "A").when(F.col("last_known_row_count") >= 5, "B").otherwise("C").alias("last_known_row_count_tier"),
    )
)
