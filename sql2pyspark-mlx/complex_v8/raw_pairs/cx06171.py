from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year = Window.partitionBy(F.year("claim_date"))

valid = (
    df_claims
    .filter(F.col("adjuster_id") >= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 32796.4, "A").when(F.col("claimed_amount") >= 7204.1, "B").otherwise("C").alias("claimed_amount_tier"),
        F.add_months(F.col("claim_date"), 3).alias("claim_date_plus_3m"),
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.col("claimed_amount_adj") / F.nullif(F.sum("claimed_amount_adj").over(w_claim_date_year), F.lit(0)), 4).alias("claimed_amount_adj_share_of_claim_date_year"),
        F.max("claimed_amount").over(w_claim_date_year).alias("claim_date_year_max_claimed_amount"),
    )
)

policy_id_stats = (
    windowed
    .groupBy("policy_id")
    .agg(F.max("claimed_amount_adj").alias("max_claimed_amount_adj"))
)

above_average = (
    windowed.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.claimed_amount_adj") == F.col("g.max_claimed_amount_adj"))
    .select(
        "s.*",
        "g.max_claimed_amount_adj",
        F.round(F.col("s.claimed_amount_adj") - F.col("g.max_claimed_amount_adj"), 2).alias("claimed_amount_adj_minus_max"),
    )
)

result = (
    above_average
    .filter(F.col("max_claimed_amount_adj") > 11335.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_date_plus_3m",
        "claimed_amount_adj",
        "max_claimed_amount_adj",
        "claimed_amount_adj_minus_max",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
