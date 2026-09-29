from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_reported_at_year = Window.partitionBy("policy_id", F.year("reported_at"))

valid_claims = (
    df_claims
    .filter((F.col("status").isin("OPEN", "PAID", "APPROVED")) & (F.col("approved_amount").between(1273.7, 23487.5)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

with_windows = (
    valid_claims
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_policy_id_reported_at_year)).alias("distinct_claim_type_in_policy_id_reported_at_yea"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_reported_at_year)).alias("distinct_adjuster_id_in_policy_id_reported_at_ye"),
        F.count(F.lit(1)).over(w_policy_id_reported_at_year).alias("rows_in_policy_id_reported_at_year"),
    )
)

labeled = (
    with_windows
    .filter(F.col("rows_in_policy_id_reported_at_year") < 11)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "distinct_claim_type_in_policy_id_reported_at_yea",
        "distinct_adjuster_id_in_policy_id_reported_at_ye",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
    )
)

annotated = (
    labeled
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "distinct_claim_type_in_policy_id_reported_at_yea",
        "distinct_adjuster_id_in_policy_id_reported_at_ye",
        "claimed_amount_filled",
        F.year(F.col("claim_date")).alias("claim_date_year"),
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

labeled_2 = (
    annotated
    .filter(F.col("claimed_amount") >= 19337.6)
    .withColumn("claim_date_month_end", F.last_day(F.col("claim_date")))
)

screened = (
    labeled_2
    .crossJoin(labeled_2.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

result = (
    screened
    .select(
        "id",
        "claimed_amount",
        "distinct_claim_type_in_policy_id_reported_at_yea",
        "distinct_adjuster_id_in_policy_id_reported_at_ye",
        "claimed_amount_filled",
        "claim_date_year",
    )
    .orderBy(
        F.col("claimed_amount_filled").asc(),
        F.col("claim_date_year").asc(),
        "id",
    )
    .limit(5)
)
