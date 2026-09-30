from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_policy_id_claim_date_rank_by = Window.partitionBy("policy_id", "claim_date_rank").orderBy("claimed_amount", "id")
w_all_by_reported_at_dense_ran = Window.orderBy(F.col("reported_at_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_year = Window.partitionBy(F.year("reported_at"))
w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_all_by_distinct_adjuster_ids = Window.orderBy("distinct_adjuster_ids")
w_all_by_distinct_adjuster_ids_2 = Window.orderBy("distinct_adjuster_ids", "reported_at_quintile", "claim_date_month", "distinct_policy_id_in_claim_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_ids_distin = Window.partitionBy("distinct_adjuster_ids", "distinct_policy_id_in_claim_type")

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_all_by_reported_at).alias("reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_type_by_reported_at)).alias("claimed_amount_change"),
        F.row_number().over(w_claim_type_by_reported_at).alias("seq_no"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.max("approved_amount").over(w_policy_id_by_claim_date_runn).alias("max_approved_amount_so_far"),
        F.ntile(5).over(w_claim_type_by_reported_at).alias("reported_at_quintile"),
        F.first("claimed_amount", ignorenulls=True).over(w_policy_id_by_claim_date_runn).alias("first_known_claimed_amount"),
        F.rank().over(w_policy_id_by_claim_date).alias("claim_date_rank"),
        F.row_number().over(w_policy_id_by_claim_date).alias("claim_date_seq"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.lag("claimed_amount_change", 3).over(w_policy_id_claim_date_rank_by).alias("prev3_claimed_amount_change"),
        F.first("first_known_claimed_amount").over(w_all_by_reported_at_dense_ran).alias("first_first_known_claimed_amount"),
        F.size(F.collect_set("policy_id").over(w_reported_at_year)).alias("distinct_policy_id_in_reported_at_year"),
        F.size(F.collect_set("adjuster_id").over(w_claim_date_month)).alias("distinct_adjuster_id_in_claim_date_month"),
    )
)

summary = (
    with_trends
    .filter(F.col("approved_amount") >= 9389.3)
    .groupBy(
        "reported_at_quintile",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "distinct_policy_id_in_claim_type",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.round(F.avg("max_approved_amount_so_far"), 2).alias("avg_max_approved_amount_so_far"),
        F.max("distinct_policy_id_in_reported_at_year").alias("max_distinct_policy_id_in_reported_at_year_check"),
    )
    .filter(F.col("max_distinct_policy_id_in_reported_at_year_check") > 3)
    .drop("max_distinct_policy_id_in_reported_at_year_check")
)

with_trends_2 = (
    summary
    .select(
        "*",
        F.rank().over(w_all_by_distinct_adjuster_ids).alias("distinct_adjuster_ids_rank"),
        F.nth_value("reported_at_quintile", 2).over(w_all_by_distinct_adjuster_ids_2).alias("second_reported_at_quintile"),
        F.min("n_rows").over(w_distinct_adjuster_ids_distin).alias("distinct_adjuster_ids_distinct_policy_id_in_clai"),
    )
)

result = (
    with_trends_2
    .filter(F.col("claim_date_month") < F.lit("2025-02-13 09:00:00").cast("timestamp"))
    .select(
        "reported_at_quintile",
        "claim_date_month",
        "distinct_policy_id_in_claim_type",
        "n_rows",
        "distinct_adjuster_ids",
        "avg_max_approved_amount_so_far",
        "distinct_adjuster_ids_rank",
        F.hour(F.col("claim_date_month")).alias("claim_date_month_hour"),
    )
)
