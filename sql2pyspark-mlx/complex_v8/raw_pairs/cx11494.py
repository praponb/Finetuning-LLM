from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_claim_date_month = Window.partitionBy(F.month("last_claim_date"))
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

annotated = (
    matched
    .filter(F.col("premium") <= 3532.7)
    .select(
        "*",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.when(F.col("claimed_amount") >= 32796.4, "large").when(F.col("claimed_amount") >= 13635.1, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_stats = (
    annotated.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.min_claim_approved_amount", "c.last_claim_date")
)

with_trends = (
    with_stats
    .select(
        "*",
        F.size(F.collect_set("adjuster_id").over(w_last_claim_date_month)).alias("distinct_adjuster_id_in_last_claim_date_month"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_last_claim_date_month), 2).alias("claimed_amount_vs_last_claim_date_month_avg"),
        F.ntile(4).over(w_policy_id_by_claimed_amount).alias("claimed_amount_quartile"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["claimed_amount_quartile", "adjuster_id"], ["claimed_amount_quartile"], ["adjuster_id"]],
        "claimed_amount_quartile", "adjuster_id",
    )
    .agg(
        F.grouping("claimed_amount_quartile").alias("is_total_claimed_amount_quartile"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)
