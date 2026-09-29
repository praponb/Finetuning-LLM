from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_claim_date_month_by_cl = Window.partitionBy(F.date_trunc("month", "claim_claim_date")).orderBy(F.col("claim_reported_at").desc(), "id", "claim_id")
w_claim_id_policy_id = Window.partitionBy("claim_id", "policy_id")
w_claim_claim_date_month_by_cl_2 = Window.partitionBy(F.date_trunc("month", "claim_claim_date")).orderBy(F.col("claim_reported_at").desc())
w_claim_claim_date_month_by_cl_3 = Window.partitionBy(F.date_trunc("month", "claim_claim_date")).orderBy(F.col("claim_reported_at").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

detail_rows_claims = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.approved_amount",
        "s.adjuster_id",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

with_windows = (
    detail_rows_claims
    .select(
        "*",
        F.lead("claim_reported_at", 3).over(w_claim_claim_date_month_by_cl).alias("next3_claim_reported_at"),
        F.size(F.collect_set("adjuster_id").over(w_claim_id_policy_id)).alias("distinct_adjuster_id_in_claim_id_policy_id"),
        F.rank().over(w_claim_claim_date_month_by_cl_2).alias("claim_reported_at_rank"),
        F.size(F.collect_set("claim_type").over(w_claim_id_policy_id)).alias("distinct_claim_type_in_claim_id_policy_id"),
        F.min("claim_approved_amount").over(w_claim_claim_date_month_by_cl_3).alias("min_claim_approved_amount_so_far"),
    )
)

result = (
    with_windows
    .filter(F.col("min_claim_approved_amount_so_far") >= 175000.0)
    .select(
        "id",
        "adjuster_id",
        "claim_id",
        "claim_approved_amount",
        "claim_reported_at",
        "claim_type",
        "claim_claim_date",
        "next3_claim_reported_at",
        "distinct_adjuster_id_in_claim_id_policy_id",
        "claim_reported_at_rank",
        "distinct_claim_type_in_claim_id_policy_id",
        "min_claim_approved_amount_so_far",
        F.date_trunc("year", F.col("next3_claim_reported_at")).alias("next3_claim_reported_at_year"),
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last(), "claim_type")
)
