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

latest_per_key = (
    matched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

per_group = (
    latest_per_key
    .filter(F.col("claim_type") != "theft")
    .groupBy("adjuster_id", "claim_type")
    .agg(
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claimed_amount_to_premium_ratio"),
        F.sum(F.when(F.col("product") == "AUTO", 1).otherwise(0)).alias("n_product_auto"),
        F.round(100 * F.count(F.when(F.col("claim_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_claim_type_is_not_null"),
        F.sum(F.when(F.col("claim_type") == "fire", F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_claim_type_fire"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

joined_adjuster = (
    per_group.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Travel"), "left")
    .select(
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount_to_premium_ratio",
        "s.n_product_auto",
        "s.pct_claim_type_is_not_null",
        "s.claimed_amount_claim_type_fire",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    joined_adjuster
    .select(
        "adjuster_id",
        "claim_type",
        "n_product_auto",
        "pct_claim_type_is_not_null",
        "claimed_amount_claim_type_fire",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_team")), " ", "_")).alias("adjuster_team_slug"),
    )
    .orderBy(F.col("adjuster_team_slug").desc(), "adjuster_id", "claim_type")
    .limit(20)
)
