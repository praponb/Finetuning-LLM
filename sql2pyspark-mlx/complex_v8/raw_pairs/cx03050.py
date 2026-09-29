from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

with_activity = (
    recent_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") < F.lit("2026-08-31").cast("date")), "left_semi")
)

without_matches = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.year(F.col("c.claim_date")) == 2024), "left_anti")
    .filter(F.col("holder_id") >= 10)
)

result = (
    without_matches
    .groupingSets(
        [["status", "holder_id"], ["status"], ["holder_id"]],
        "status", "holder_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("n_status_active"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
