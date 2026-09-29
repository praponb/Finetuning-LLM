from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").asc_nulls_last())
w_claim_date_year = Window.partitionBy(F.year("claim_date"))

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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

ranked = (
    in_window
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
        F.round(F.avg("claimed_amount").over(w_claim_date_year), 2).alias("claim_date_year_avg_claimed_amount"),
    )
)

product_stats = (
    ranked
    .groupBy("product")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

outliers = (
    ranked.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

derived = (
    outliers
    .filter(F.col("claimed_amount_minus_avg") < 10000.0)
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "claimed_amount_percent_rank",
        "claimed_amount_minus_avg",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.add_months(F.col("claim_date"), 3).alias("claim_date_plus_3m"),
        F.when(F.col("claimed_amount_minus_avg") >= 10000.0, "A").when(F.col("claimed_amount_minus_avg") >= -25000.0, "B").otherwise("C").alias("claimed_amount_minus_avg_tier"),
    )
)

result = (
    derived
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "claimed_amount_percent_rank",
        "claimed_amount_minus_avg",
        "product_prefix",
        "claim_date_plus_3m",
        "claimed_amount_minus_avg_tier",
    )
    .orderBy("claimed_amount")
)
