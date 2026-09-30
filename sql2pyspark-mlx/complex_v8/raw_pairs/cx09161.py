from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_status_len_bucket_tie = Window.orderBy(F.col("status_len_bucket").desc())
w_all_by_status_len_bucket = Window.orderBy(F.col("status_len_bucket").desc(), "id")
w_status_len_reported_at_to_da = Window.partitionBy("status_len", F.to_date("reported_at"))

filtered = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 2)) & (F.col("claimed_amount").between(11235.6, 45826.9)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        (F.floor(F.col("status_len") / 2) * 2).alias("status_len_bucket"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.coalesce(F.col("status_len"), F.lit(0)).alias("status_len_filled"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3), "left_semi")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.dense_rank().over(w_all_by_status_len_bucket_tie).alias("status_len_bucket_dense_rank"),
        F.lag("status_len", 1, 0).over(w_all_by_status_len_bucket).alias("prev_status_len"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_len_reported_at_to_da), 2).alias("claimed_amount_vs_status_len_reported_at_to_date"),
        F.dense_rank().over(w_all_by_status_len_bucket).alias("status_len_bucket_dense_rank_2"),
    )
)

exploded_detail = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") > 18364.1), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.status_len_filled",
        "s.status_len_bucket_dense_rank",
        "s.prev_status_len",
        "s.status_len_bucket_dense_rank_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

joined_adjuster = (
    exploded_detail.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Travel", "Motor", "Property")), "left")
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

result = (
    joined_adjuster
    .groupBy("status_len_filled", "status_len_bucket_dense_rank_2")
    .pivot("status", ["IN_REVIEW", "REJECTED", "OPEN", "APPROVED"])
    .agg(F.min("adjuster_level"))
)
