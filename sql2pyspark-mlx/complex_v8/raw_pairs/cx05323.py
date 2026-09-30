from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_date").desc(), F.col("id").desc())

covered = (
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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

unique_rows = (
    covered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    unique_rows
    .crossJoin(unique_rows.agg(F.min("claimed_amount").alias("overall_min_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_min_claimed_amount"))
    .drop("overall_min_claimed_amount")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "product",
        "premium",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
    .orderBy(
        F.col("claimed_amount").desc(),
        F.col("claim_date").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
