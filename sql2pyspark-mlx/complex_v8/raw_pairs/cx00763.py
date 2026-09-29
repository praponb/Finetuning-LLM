from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group = Window.partitionBy("status_group")
w_all_by_approved_amount_adj = Window.orderBy(F.col("approved_amount_adj").desc(), "id")
w_all_by_approved_amount_adj_r = Window.orderBy(F.col("approved_amount_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_claims
    .filter(F.col("claimed_amount") >= 4554.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.when(F.col("status").isin("REJECTED", "APPROVED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

enriched = (
    base
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status_group",
        F.round(F.col("approved_amount") * 1.07, 2).alias("approved_amount_adj"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.min("claim_date").alias("first_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

windowed = (
    with_stats
    .select(
        "*",
        F.min("approved_amount_adj").over(w_status_group).alias("status_group_min_approved_amount_adj"),
        F.ntile(4).over(w_all_by_approved_amount_adj).alias("approved_amount_adj_quartile"),
        F.round(F.col("claimed_amount_bucket") / F.nullif(F.sum("claimed_amount_bucket").over(w_status_group), F.lit(0)), 4).alias("claimed_amount_bucket_share_of_status_group"),
        F.last("claim_count", ignorenulls=True).over(w_all_by_approved_amount_adj_r).alias("last_known_claim_count"),
        F.row_number().over(w_all_by_approved_amount_adj).alias("rn"),
    )
)

result = (
    windowed
    .groupBy("first_claim_date", "approved_amount_adj_quartile")
    .pivot("claim_type", ["medical", "theft", "collision"])
    .agg(F.avg("claimed_amount_bucket_share_of_status_group"))
)
