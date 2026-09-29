from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "adjuster_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())

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
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

latest_per_key = (
    matched
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .rollup("policy_id", "adjuster_id", "product")
    .agg(
        F.sum(F.when(F.col("claim_type") == "medical", 1).otherwise(0)).alias("n_claim_type_medical"),
    )
    .select(
        "policy_id",
        "adjuster_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "n_claim_type_medical",
    )
)
