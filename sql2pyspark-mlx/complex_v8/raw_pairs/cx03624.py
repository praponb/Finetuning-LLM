from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claim_date").alias("first_claim_date"))
)

profiled = (
    cleaned_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_date")
)

with_activity = (
    profiled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "HEALTH"), "left_anti")
    .filter(F.col("status").isNotNull())
)

result = (
    with_activity
    .groupingSets(
        [["status", "holder_id"], ["status"], ["holder_id"], []],
        "status", "holder_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("n_status_active"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        "n_status_active",
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
