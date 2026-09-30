from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type").orderBy(F.col("premium").desc(), F.col("id").desc())

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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

deduped = (
    matched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    deduped
    .groupBy("adjuster_id", "product", "claim_type")
    .agg(
        F.sum(F.when(~F.col("product").isin("AUTO", "TRAVEL"), 1).otherwise(0)).alias("n_product_not_in_auto_travel"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claimed_amount_to_premium_ratio"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.min("premium").alias("min_premium"),
        F.avg("claimed_amount").alias("avg_claimed_amount_check"),
    )
    .filter(F.col("avg_claimed_amount_check") > 7204.1)
    .drop("avg_claimed_amount_check")
)

result = (
    totals
    .filter(F.size(F.col("claim_type_list")) >= 2)
    .select(
        "adjuster_id",
        "claim_type",
        "n_product_not_in_auto_travel",
        "claimed_amount_to_premium_ratio",
        "claim_type_list",
        "min_premium",
        F.coalesce(F.col("min_premium"), F.lit(0.0)).alias("min_premium_filled"),
    )
    .orderBy(F.col("min_premium").desc())
)
