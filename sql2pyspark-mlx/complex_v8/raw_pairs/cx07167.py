from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy("claim_date", "id", "claim_id")
w_claim_id = Window.partitionBy("claim_id")

scoped = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2027-03-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

enriched = (
    scoped
    .filter(F.col("status_ref").isNotNull())
    .select("*", F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"))
)

with_children = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status_ref",
        "s.premium_bucket",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
    )
)

metrics = (
    with_children
    .select(
        "*",
        F.row_number().over(w_all_by_claim_date).alias("rn"),
        F.sum("claim_claimed_amount").over(w_claim_id).alias("claim_id_sum_claim_claimed_amount"),
        F.round(F.col("claim_claimed_amount") - F.avg("claim_claimed_amount").over(w_claim_id), 2).alias("claim_claimed_amount_vs_claim_id_avg"),
        F.count(F.lit(1)).over(w_claim_id).alias("rows_in_claim_id"),
        F.ntile(10).over(w_all_by_claim_date).alias("claim_date_decile"),
    )
)

result = (
    metrics
    .groupingSets(
        [["holder_id", "rn"], ["holder_id"], ["rn"]],
        "holder_id", "rn",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("claim_date_decile"), 2).alias("stddev_claim_date_decile"),
        F.sum("claim_claimed_amount").alias("total_claim_claimed_amount"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
)
