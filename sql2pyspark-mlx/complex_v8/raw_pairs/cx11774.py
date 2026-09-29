from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_avg_claimed_amount = Window.orderBy("avg_claimed_amount", "status")
w_status_avg_claimed_amount_ra = Window.partitionBy("status", "avg_claimed_amount_rank")
w_status_by_avg_claimed_amount = Window.partitionBy("status").orderBy("avg_claimed_amount", "status").rowsBetween(-1, -1)
w_status_by_avg_claimed_amount_2 = Window.partitionBy("status").orderBy("avg_claimed_amount", "status")

joined_ranges = (
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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

stats = (
    joined_ranges
    .groupBy("status")
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.rank().over(w_all_by_avg_claimed_amount).alias("avg_claimed_amount_rank"),
    )
)

with_trends = (
    ranked
    .select(
        "status",
        "total_premium",
        "avg_claimed_amount",
        "last_claim_date",
        "rows_in_status",
        "avg_claimed_amount_rank",
        F.max("avg_claimed_amount_rank").over(w_status_avg_claimed_amount_ra).alias("status_avg_claimed_amount_rank_max_avg_claimed_a"),
        F.sum("avg_claimed_amount_rank").over(w_status_by_avg_claimed_amount).alias("avg_claimed_amount_rank_sum_1_prior"),
        F.round(F.col("avg_claimed_amount") / F.nullif(F.sum("avg_claimed_amount").over(w_status_avg_claimed_amount_ra), F.lit(0)), 4).alias("avg_claimed_amount_share_of_status_avg_claimed_a"),
        F.lag("last_claim_date").over(w_status_by_avg_claimed_amount_2).alias("prev_last_claim_date"),
        F.lead("total_premium").over(w_status_by_avg_claimed_amount_2).alias("next_total_premium"),
    )
)

result = (
    with_trends
    .cube("status", "avg_claimed_amount_rank")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("total_premium"), 2).alias("avg_total_premium"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("avg_claimed_amount_rank").asc_nulls_last(),
    )
)
