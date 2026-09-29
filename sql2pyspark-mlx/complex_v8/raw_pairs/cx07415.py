from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_status_len_by_status_len_tie = Window.partitionBy("status_len").orderBy("status_len")
w_status_len_by_status_len_ful = Window.partitionBy("status_len").orderBy("status_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_trends = (
    recent_policys
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "premium_distance",
        "status_len",
        F.round(F.col("status_len") - F.avg("status_len").over(w_holder_id), 2).alias("status_len_vs_holder_id_avg"),
        F.rank().over(w_status_len_by_status_len_tie).alias("status_len_rank"),
        F.nth_value("status_len", 2).over(w_status_len_by_status_len_ful).alias("second_status_len"),
        F.round(F.cume_dist().over(w_status_len_by_status_len_tie), 3).alias("status_len_cume_dist"),
    )
)

enriched = (
    with_trends
    .filter(F.col("start_date") >= F.lit("2025-01-05").cast("date"))
    .withColumn("status_len_cume_dist_adj", F.round(F.col("status_len_cume_dist") * 1.1, 2))
)

detail_rows = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.premium_distance",
        "s.status_len_rank",
        "s.second_status_len",
        "s.status_len_cume_dist",
        "s.status_len_cume_dist_adj",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "premium",
        "start_date",
        "premium_distance",
        "status_len_rank",
        "second_status_len",
        "status_len_cume_dist",
        "status_len_cume_dist_adj",
        "claim_approved_amount",
        "claim_claimed_amount",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
    .orderBy("claim_approved_amount", F.col("start_date").desc_nulls_first())
)
