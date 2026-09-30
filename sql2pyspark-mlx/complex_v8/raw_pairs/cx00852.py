from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2027-01-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
    )
)

labeled = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "end_date_quarter",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") >= 1132.1), "left_semi")
)

with_children = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "collision"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date_quarter",
        "s.days_start_date_to_end_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    with_children
    .groupBy("holder_id")
    .pivot("claim_type", ["collision", "flood"])
    .agg(F.sum("end_date_quarter"))
    .orderBy("holder_id")
)
