from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date_week").desc(), F.col("id").desc())
w_policy_id_claim_date_year_by = Window.partitionBy("policy_id", F.year("claim_date")).orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_week_month = Window.partitionBy(F.month("claim_date_week"))

valid_claims = (
    df_claims
    .filter((F.col("claim_type") != "delay") & (F.col("policy_id") <= 23))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
)

unique_rows = (
    valid_claims
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details_policy = (
    unique_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_date_week",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

metrics = (
    combined_details_policy
    .select(
        "*",
        F.last("claimed_amount", ignorenulls=True).over(w_policy_id_claim_date_year_by).alias("last_known_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_week_month), 2).alias("claimed_amount_vs_claim_date_week_month_avg"),
        F.count(F.lit(1)).over(w_claim_date_week_month).alias("rows_in_claim_date_week_month"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "claim_date_week",
        "policy_premium",
        "policy_end_date",
        "claimed_amount_vs_claim_date_week_month_avg",
        F.round(F.col("policy_premium") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("policy_premium_per_claimed_amount"),
        F.datediff(F.col("policy_end_date"), F.col("claim_date")).alias("days_claim_date_to_policy_end_date"),
    )
    .orderBy(
        F.col("claim_date_week").desc(),
        F.col("policy_premium_per_claimed_amount").desc_nulls_first(),
    )
)
