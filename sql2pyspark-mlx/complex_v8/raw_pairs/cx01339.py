from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

with_details_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date") >= F.lit("2026-01-24").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.product",
        "s.premium",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

latest = (
    with_details_policy
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "premium",
        "policy_end_date",
        F.abs(F.col("approved_amount") - 4554.8).alias("approved_amount_distance"),
        F.year(F.col("policy_end_date")).alias("policy_end_date_year"),
    )
    .orderBy(F.col("policy_end_date_year").desc())
)
