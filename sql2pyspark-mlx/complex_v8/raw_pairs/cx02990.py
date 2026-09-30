from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_or_unknown = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("end_date").desc(), "id")
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_status_or_unknown_2 = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter((~F.col("status").isin("LAPSED", "CANCELLED")) | (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

labeled = (
    filtered_policys
    .filter(~F.col("status").isin("CANCELLED", "ACTIVE"))
    .withColumn("status_or_unknown", F.coalesce(F.col("status"), F.lit("UNKNOWN")))
)

with_trends = (
    labeled
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        F.lag("end_date").over(w_holder_id_status_or_unknown).alias("prev_end_date"),
        F.lag("end_date", 2).over(w_holder_id_by_end_date).alias("prev2_end_date"),
        F.nth_value("end_date", 2).over(w_holder_id_by_end_date_full).alias("second_end_date"),
        F.nth_value("end_date", 2).over(w_holder_id_status_or_unknown_2).alias("second_end_date_2"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.second_end_date_2",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

exploded_detail = (
    joined.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.hour(F.col("c.reported_at")).between(9, 14)), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

exploded_detail_2 = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.claim_id",
        "s.claim_date",
        "s.claim_claimed_amount",
        "s.claim_reported_at",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.status").alias("claim_status"),
    )
)

without_matches = (
    exploded_detail_2.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2026-09-23").cast("date"))) & (F.col("p.premium") >= 2033.8), "left_anti")
)

result = (
    without_matches
    .filter(F.col("claim_claimed_amount") <= 34763.4)
    .select(
        "id",
        "holder_id",
        "claim_id",
        "claim_claimed_amount",
        "claim_reported_at",
        "claim_id_2",
        F.round(F.col("claim_claimed_amount") * 1.5, 2).alias("claim_claimed_amount_adj"),
        F.dayofweek(F.col("claim_reported_at")).alias("claim_reported_at_dayofweek"),
    )
    .orderBy(F.col("claim_reported_at").desc())
)
