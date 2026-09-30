from pyspark.sql import functions as F

recent = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled_2"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

screened = (
    recent
    .crossJoin(recent.agg(F.min("premium_filled").alias("overall_min_premium_filled")))
    .filter(F.col("premium_filled") > F.col("overall_min_premium_filled"))
    .drop("overall_min_premium_filled")
)

screened_2 = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isNotNull()) & (F.col("c.claim_date") < F.lit("2025-03-31").cast("date")), "left_anti")
)

qualified = (
    screened_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") != "REJECTED") & (F.col("c.approved_amount") > 2887.3), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["status", "holder_id"], ["status"], ["holder_id"], []],
        "status", "holder_id",
    )
    .agg(F.count("*").alias("n_rows"))
    .orderBy(F.col("status").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
