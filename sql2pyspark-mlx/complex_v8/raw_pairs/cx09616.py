from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("reported_at").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 1.07, 2).alias("approved_amount_adj"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("approved_amount").alias("total_claim_approved_amount"))
)

profiled = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.total_claim_approved_amount")
)

with_activity = (
    profiled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") > 0.0), "left_anti")
    .filter(F.col("claim_type_ref").isNotNull())
)

latest_per_key = (
    with_activity
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("policy_id", "reported_at")
    .pivot("status", ["OPEN", "PAID", "REJECTED", "IN_REVIEW"])
    .agg(F.min("approved_amount").alias("min_v"), F.sum("approved_amount").alias("sum_v"))
    .orderBy("policy_id", "reported_at")
)
