from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")
w_cnt_status_by_first_reported = Window.partitionBy("cnt", "status").orderBy("first_reported_at_hour", "claim_type", "status")
w_claim_type_by_stddev_avg_app = Window.partitionBy("claim_type").orderBy("stddev_avg_approved_amount_plus_claimed_amount", "claim_type", "status")
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_cnt_status_by_cnt_share_of_c = Window.partitionBy("cnt", "status").orderBy(F.col("cnt_share_of_cnt").desc(), "claim_type", "status")
w_cnt_status_by_cnt_share_of_c_2 = Window.partitionBy("cnt", "status").orderBy(F.col("cnt_share_of_cnt").desc(), "claim_type", "status").rowsBetween(-1, Window.currentRow)
w_status_stddev_avg_approved_a = Window.partitionBy("status", "stddev_avg_approved_amount_plus_claimed_amount_s")
w_claim_type_cnt_by_cnt_avg_cn = Window.partitionBy("claim_type", "cnt").orderBy(F.col("cnt_avg_cnt_vs_status_claim_type_avg").desc(), "claim_type", "status")

cleaned_claims = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(2, 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
        F.when(F.col("claimed_amount") >= 33690.0, "HIGH").when(F.col("claimed_amount") >= 18924.4, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

claimed_amount_tier_stats = (
    cleaned_claims
    .groupBy("claimed_amount_tier")
    .agg(F.avg("approved_amount_plus_claimed_amount").alias("avg_approved_amount_plus_claimed_amount"))
)

compared = (
    cleaned_claims.alias("s")
    .join(claimed_amount_tier_stats.alias("g"), F.col("g.claimed_amount_tier") == F.col("s.claimed_amount_tier"), "inner")
    .filter(F.col("s.approved_amount_plus_claimed_amount") > F.col("g.avg_approved_amount_plus_claimed_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount_plus_claimed_amount",
        F.round(F.col("s.approved_amount_plus_claimed_amount") - F.col("g.avg_approved_amount_plus_claimed_amount"), 2).alias("approved_amount_plus_claimed_amount_minus_avg"),
    )
)

per_group = (
    compared
    .groupBy("claim_type", "status")
    .agg(
        F.count("*").alias("cnt"),
        F.min("reported_at_hour").alias("first_reported_at_hour"),
        F.round(F.stddev("avg_approved_amount_plus_claimed_amount"), 2).alias("stddev_avg_approved_amount_plus_claimed_amount"),
    )
    .filter(F.col("cnt") >= 2)
)

metrics = (
    per_group
    .select(
        "*",
        F.round(F.avg("cnt").over(w_cnt), 2).alias("cnt_avg_cnt"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_cnt), F.lit(0)), 4).alias("cnt_share_of_cnt"),
        F.lead("first_reported_at_hour", 2).over(w_cnt_status_by_first_reported).alias("next2_first_reported_at_hour"),
        F.lag("first_reported_at_hour").over(w_claim_type_by_stddev_avg_app).alias("prev_first_reported_at_hour"),
        F.row_number().over(w_claim_type_by_stddev_avg_app).alias("stddev_avg_approved_amount_plus_claimed_amount_s"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.col("cnt_avg_cnt") - F.avg("cnt_avg_cnt").over(w_status_claim_type), 2).alias("cnt_avg_cnt_vs_status_claim_type_avg"),
        F.lag("cnt_share_of_cnt").over(w_cnt_status_by_cnt_share_of_c).alias("prev_cnt_share_of_cnt"),
        F.round(F.avg("cnt_share_of_cnt").over(w_cnt_status_by_cnt_share_of_c_2), 2).alias("cnt_share_of_cnt_avg_2"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.sum("cnt_share_of_cnt_avg_2").over(w_status_stddev_avg_approved_a).alias("status_stddev_avg_approved_amount_plus_claimed_a"),
        F.dense_rank().over(w_claim_type_cnt_by_cnt_avg_cn).alias("cnt_avg_cnt_vs_status_claim_type_avg_dense_rank"),
    )
)

result = (
    ranked
    .groupingSets(
        [["claim_type", "stddev_avg_approved_amount_plus_claimed_amount_s"], ["claim_type"]],
        "claim_type", "stddev_avg_approved_amount_plus_claimed_amount_s",
    )
    .agg(
        F.grouping("claim_type").alias("is_total_claim_type"),
        F.round(F.stddev("cnt"), 2).alias("stddev_cnt"),
        F.sum("stddev_avg_approved_amount_plus_claimed_amount").alias("total_stddev_avg_approved_amount_plus_claimed_am"),
    )
)
