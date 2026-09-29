from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())

filtered = (
    df_claims
    .filter(F.col("claimed_amount") > 18364.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 11215.8).alias("approved_amount_distance"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date").isNotNull()), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

qualified = (
    joined
    .crossJoin(joined.agg(F.max("adjuster_level").alias("overall_max_adjuster_level")))
    .filter(F.col("adjuster_level") < F.col("overall_max_adjuster_level"))
    .drop("overall_max_adjuster_level")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("claim_type", "policy_id", "adjuster_level")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.array_sort(F.collect_set("adjuster_team")).alias("adjuster_team_list"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claimed_amount_to_approved_amount_ratio"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)
