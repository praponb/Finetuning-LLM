from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_status").orderBy(F.col("adjuster_level").desc(), F.col("id").desc())

cleaned_claims = (
    df_claims
    .filter((F.col("status") == "APPROVED") & (F.col("claim_date") >= F.lit("2024-06-26").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 23207.7, "A").when(F.col("approved_amount") >= 4868.7, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

with_parent = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.approved_amount_tier",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

unique_rows = (
    with_parent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    unique_rows
    .filter(F.col("approved_amount_tier").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount_tier",
        "policy_status",
        "policy_holder_id",
        "adjuster_level",
        "adjuster_name",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

result = (
    derived
    .groupingSets(
        [["adjuster_level", "policy_id"], ["adjuster_level"], ["policy_id"], []],
        "adjuster_level", "policy_id",
    )
    .agg(
        F.min("adjuster_level").alias("min_adjuster_level"),
        F.sum(F.when(F.col("policy_status") == "ACTIVE", 1).otherwise(0)).alias("n_policy_status_active"),
        F.round(F.avg("adjuster_level"), 2).alias("avg_adjuster_level"),
    )
)
