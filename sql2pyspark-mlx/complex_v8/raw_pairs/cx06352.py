from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_approved_amount_fu = Window.partitionBy("status").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_adjuster_id_status_by_claime = Window.partitionBy("adjuster_id", "status").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_policy_id_by_appr = Window.partitionBy("claim_type", "policy_id").orderBy("approved_amount", "id")
w_next2_claim_date_year_status = Window.partitionBy(F.year("next2_claim_date"), "status")
w_next2_claim_date_month_statu = Window.partitionBy(F.month("next2_claim_date"), "status").orderBy("claimed_amount", "id")
w_next2_claim_date_month_statu_2 = Window.partitionBy(F.month("next2_claim_date"), "status").orderBy("claimed_amount", "id").rowsBetween(-1, Window.currentRow)
w_distinct_adjuster_id_in_stat = Window.partitionBy("distinct_adjuster_id_in_status", F.year("second_claim_date")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_id_in_stat_2 = Window.partitionBy("distinct_adjuster_id_in_status", F.year("second_claim_date")).orderBy(F.col("claimed_amount").desc(), "id")

valid_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-04-11").cast("date")) & (F.col("approved_amount") <= 14595.6))
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
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

sequenced = (
    valid_claims
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_label",
        "claimed_amount_minus_approved_amount",
        F.nth_value("claim_date", 2).over(w_status_by_approved_amount_fu).alias("second_claim_date"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_status_by_claime).alias("first_known_claimed_amount"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.lead("claim_date", 2).over(w_claim_type_policy_id_by_appr).alias("next2_claim_date"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_date_label",
        "claimed_amount_minus_approved_amount",
        "second_claim_date",
        "distinct_adjuster_id_in_status",
        "approved_amount_vs_status_avg",
        "first_known_claimed_amount",
        "rows_in_status",
        F.max("first_known_claimed_amount").over(w_next2_claim_date_year_status).alias("next2_claim_date_year_status_max_first_known_cla"),
        F.lead("claimed_amount").over(w_next2_claim_date_month_statu).alias("next_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_next2_claim_date_year_status)).alias("distinct_claim_type_in_next2_claim_date_year_sta"),
        F.count(F.lit(1)).over(w_next2_claim_date_year_status).alias("rows_in_next2_claim_date_year_status"),
        F.sum("first_known_claimed_amount").over(w_next2_claim_date_month_statu_2).alias("first_known_claimed_amount_sum_2"),
    )
)

sequenced_2 = (
    metrics
    .select(
        "*",
        F.nth_value("first_known_claimed_amount", 2).over(w_distinct_adjuster_id_in_stat).alias("second_first_known_claimed_amount"),
        F.ntile(10).over(w_distinct_adjuster_id_in_stat_2).alias("claimed_amount_decile"),
        F.ntile(4).over(w_distinct_adjuster_id_in_stat_2).alias("claimed_amount_quartile"),
        F.round(F.percent_rank().over(w_distinct_adjuster_id_in_stat_2), 3).alias("claimed_amount_percent_rank"),
    )
)

result = (
    sequenced_2
    .groupBy("claimed_amount_quartile", "claim_type")
    .pivot("status", ["APPROVED", "OPEN", "IN_REVIEW", "REJECTED"])
    .agg(F.avg("rows_in_status").alias("avg_v"), F.sum("rows_in_status").alias("sum_v"))
    .orderBy("claimed_amount_quartile", "claim_type")
)
