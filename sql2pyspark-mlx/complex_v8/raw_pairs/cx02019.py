from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "product").orderBy(F.col("claim_date").desc(), F.col("id").desc())

joined_ranges = (
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

unique_rows = (
    joined_ranges
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "premium",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
