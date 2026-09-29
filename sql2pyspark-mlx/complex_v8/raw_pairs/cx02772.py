from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_claim_date_month_year_policy = Window.partitionBy(F.year("claim_date_month"), "policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_adjuster_id_by_report = Window.partitionBy("status", "adjuster_id").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date_month")).orderBy(F.col("claim_date").desc(), "id")
w_claim_date_month_year_policy_2 = Window.partitionBy(F.year("claim_date_month"), "policy_id").orderBy(F.col("claimed_amount").desc(), "id")
w_status_adjuster_id_by_report_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("reported_at").desc())
w_claim_date_month_to_date_cla = Window.partitionBy(F.to_date("claim_date_month"), "claim_type")
w_reported_at_month_claim_type = Window.partitionBy(F.date_trunc("month", "reported_at"), "claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_reported_at_month = Window.partitionBy("policy_id", F.date_trunc("month", "reported_at"))

cleaned = (
    df_claims
    .filter(F.col("policy_id") <= 17)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.round(F.col("claimed_amount_filled") / F.nullif(F.sum("claimed_amount_filled").over(w_claim_date_month), F.lit(0)), 4).alias("claimed_amount_filled_share_of_claim_date_month"),
        F.nth_value("claimed_amount", 2).over(w_claim_date_month_year_policy).alias("second_claimed_amount"),
        F.lag("claim_date_month", 2).over(w_status_adjuster_id_by_report).alias("prev2_claim_date_month"),
        F.rank().over(w_adjuster_id_claim_date_month).alias("claim_date_rank"),
        F.row_number().over(w_claim_date_month_year_policy_2).alias("rn"),
        F.last("claim_date").over(w_claim_date_month_year_policy).alias("last_claim_date"),
        F.rank().over(w_status_adjuster_id_by_report_2).alias("reported_at_rank"),
        F.count(F.lit(1)).over(w_claim_date_month_to_date_cla).alias("rows_in_claim_date_month_to_date_claim_type"),
        F.nth_value("claimed_amount_filled", 2).over(w_reported_at_month_claim_type).alias("second_claimed_amount_filled"),
        F.size(F.collect_set("status").over(w_claim_date_month)).alias("distinct_status_in_claim_date_month"),
        F.size(F.collect_set("claim_type").over(w_policy_id_reported_at_month)).alias("distinct_claim_type_in_policy_id_reported_at_mon"),
        (F.col("claimed_amount_filled") - F.lag("claimed_amount_filled").over(w_adjuster_id_claim_date_month)).alias("claimed_amount_filled_change"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_claim_type_in_policy_id_reported_at_mon") < 9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_filled",
        "claimed_amount_filled_share_of_claim_date_month",
        "second_claimed_amount",
        "prev2_claim_date_month",
        "rn",
        "last_claim_date",
        "reported_at_rank",
        "rows_in_claim_date_month_to_date_claim_type",
        "second_claimed_amount_filled",
        "distinct_status_in_claim_date_month",
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last())
)
