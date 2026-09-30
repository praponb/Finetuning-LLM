from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_by_last_reported_a = Window.partitionBy("row_count").orderBy("last_reported_at", "reported_at_month")
w_row_count_by_row_count_full = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "reported_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count = Window.partitionBy("row_count")
w_row_count_by_last_reported_a_2 = Window.partitionBy("row_count").orderBy("last_reported_at", "reported_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

line_level_claims = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

rollup_base = (
    line_level_claims
    .groupBy(F.date_trunc("month", "reported_at").alias("reported_at_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("reported_at").alias("last_reported_at"),
    )
)

with_windows = (
    rollup_base
    .select(
        "reported_at_month",
        "row_count",
        F.row_number().over(w_row_count_by_last_reported_a).alias("last_reported_at_seq"),
        F.last("last_reported_at").over(w_row_count_by_row_count_full).alias("last_last_reported_at"),
        F.lead("row_count").over(w_row_count_by_last_reported_a).alias("next_row_count"),
        F.first("row_count", ignorenulls=True).over(w_row_count_by_last_reported_a_2).alias("first_known_row_count"),
    )
)

result = (
    with_windows
    .select(
        "reported_at_month",
        "row_count",
        "last_reported_at_seq",
        "next_row_count",
        "first_known_row_count",
        (F.col("row_count") + F.col("first_known_row_count")).alias("row_count_plus_first_known_row_count"),
        F.round((F.unix_timestamp(F.col("last_last_reported_at")) - F.unix_timestamp(F.col("reported_at_month"))) / 3600, 1).alias("hours_reported_at_month_to_last_last_reported_at"),
    )
    .orderBy(F.col("first_known_row_count").asc())
)
