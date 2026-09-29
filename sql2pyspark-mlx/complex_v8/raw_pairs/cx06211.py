from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_dedup = Window.partitionBy("claim_type", "status").orderBy(F.col("claimed_amount_change").desc(), F.col("id").desc())

matched = (
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
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

metrics = (
    matched
    .select(
        "*",
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_status_by_premium)).alias("claimed_amount_change"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "claim_date",
        "claim_type",
        "product",
        "policy_id_sum_claimed_amount",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
        F.when(F.col("claimed_amount_change") >= 20000.0, "HIGH").when(F.col("claimed_amount_change") >= -25000.0, "MEDIUM").otherwise("LOW").alias("claimed_amount_change_tier"),
    )
    .orderBy("claimed_amount_change_tier")
)
