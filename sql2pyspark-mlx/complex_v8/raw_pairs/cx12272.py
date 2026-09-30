from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_product_or_unknown = Window.partitionBy("policy_id", "product_or_unknown").orderBy("claim_date")
w_policy_id_product_or_unknown_2 = Window.partitionBy("policy_id", "product_or_unknown").orderBy("claim_date", "id")

in_window = (
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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

scored = (
    in_window
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "premium",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "product_clean",
        F.dense_rank().over(w_policy_id_product_or_unknown).alias("claim_date_dense_rank"),
        F.lead("premium").over(w_policy_id_product_or_unknown_2).alias("next_premium"),
    )
)

derived = (
    metrics
    .filter(F.col("claim_date") >= F.lit("2024-04-14").cast("date"))
    .select(
        "*",
        F.abs(F.col("next_premium") - 3532.7).alias("next_premium_distance"),
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
    )
)

result = (
    derived
    .cube("claim_date_dense_rank", "product", "policy_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("claim_date_dense_rank") / F.nullif(F.sum("next_premium"), F.lit(0)), 3).alias("claim_date_dense_rank_to_next_premium_ratio"),
        F.sum(F.when(~F.col("product").isin("HEALTH", "AUTO"), F.col("claim_date_dense_rank")).otherwise(0)).alias("claim_date_dense_rank_product_not_in_health_auto"),
    )
    .select(
        "claim_date_dense_rank",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "policy_id",
        "cnt",
        "claim_date_dense_rank_to_next_premium_ratio",
        "claim_date_dense_rank_product_not_in_health_auto",
    )
    .orderBy(
        F.col("claim_date_dense_rank").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
