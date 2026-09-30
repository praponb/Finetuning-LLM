from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_reported_at_month_claim_type = Window.partitionBy(F.date_trunc("month", "reported_at"), "claim_type").orderBy("claimed_amount_plus_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date").desc(), "id")

valid = (
    df_claims
    .filter((F.col("adjuster_id") <= 4) & (F.col("claim_type") == "delay"))
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
        F.year(F.col("reported_at")).alias("reported_at_year"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.sum("approved_amount").over(w_policy_id_adjuster_id_by_cla).alias("approved_amount_sum_3"),
        F.sum("approved_amount").over(w_reported_at_month_claim_type).alias("running_approved_amount"),
        F.round(F.cume_dist().over(w_policy_id_adjuster_id_by_cla_2), 3).alias("claim_date_cume_dist"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id"),
        F.size(F.collect_set("status").over(w_adjuster_id)).alias("distinct_status_in_adjuster_id"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (~F.col("c.claim_type").isin("collision", "flood")) & (F.col("c.claim_date").isNotNull()), "left_semi")
    .filter(F.col("claimed_amount") < 38909.4)
)

without_matches = (
    with_activity
    .crossJoin(with_activity.agg(F.max("rows_in_adjuster_id").alias("overall_max_rows_in_adjuster_id")))
    .filter(F.col("rows_in_adjuster_id") < F.col("overall_max_rows_in_adjuster_id"))
    .drop("overall_max_rows_in_adjuster_id")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_year",
        "claimed_amount_plus_approved_amount",
        "status_claim_type_label",
        "rows_in_adjuster_id",
        "approved_amount_sum_3",
        "running_approved_amount",
        "claim_date_cume_dist",
        F.least(F.col("approved_amount"), F.col("rows_in_adjuster_id")).alias("least_approved_amount_rows_in_adjuster_id"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)
