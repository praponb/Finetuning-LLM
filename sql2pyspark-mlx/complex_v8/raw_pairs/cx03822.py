from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_claim_type_coun = Window.orderBy(F.col("claim_claim_type_count").asc_nulls_last())
w_all_by_claim_claim_type_coun_2 = Window.orderBy(F.col("claim_claim_type_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_claim_type_b = Window.partitionBy(F.year("claim_date"), "claim_type").orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_claim_type_coun_3 = Window.orderBy(F.col("claim_claim_type_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_claim_type_count_by_cl = Window.partitionBy("claim_claim_type_count").orderBy(F.col("claimed_amount").desc(), "id")
w_claim_claim_type_count_by_ap = Window.partitionBy("claim_claim_type_count").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_claim_claim_type_count_by_ap_2 = Window.partitionBy("claim_claim_type_count").orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_claim_date_tercile_policy_id = Window.partitionBy("claim_date_tercile", "policy_id").orderBy("claim_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("policy_id") <= 22)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

profiled = (
    prepared_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.last_claim_reported_at",
    )
)

with_trends = (
    profiled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "claimed_amount_adj",
        "claimed_amount_bucket",
        "claimed_amount_adj_2",
        "claim_count",
        "claim_claim_type_count",
        "last_claim_reported_at",
        F.round(F.cume_dist().over(w_all_by_claim_claim_type_coun), 3).alias("claim_claim_type_count_cume_dist"),
        F.nth_value("claim_count", 2).over(w_all_by_claim_claim_type_coun_2).alias("second_claim_count"),
        F.ntile(3).over(w_claim_date_year_claim_type_b).alias("claim_date_tercile"),
        F.sum("claimed_amount_adj").over(w_all_by_claim_claim_type_coun_3).alias("running_claimed_amount_adj"),
        F.ntile(5).over(w_claim_claim_type_count_by_cl).alias("claimed_amount_quintile"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.last("approved_amount", ignorenulls=True).over(w_claim_claim_type_count_by_ap).alias("last_known_approved_amount"),
        F.round(F.cume_dist().over(w_all_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.round(F.avg("claim_date_tercile").over(w_claim_claim_type_count_by_ap_2), 2).alias("claim_date_tercile_avg_3"),
        F.first("second_claim_count", ignorenulls=True).over(w_claim_date_tercile_policy_id).alias("first_known_second_claim_count"),
    )
)

result = (
    ranked
    .filter(F.col("claimed_amount_adj_2") <= 63765.0)
    .select(
        "id",
        "approved_amount",
        "status",
        "claimed_amount_bucket",
        "claimed_amount_adj_2",
        "claim_count",
        "claim_claim_type_count",
        "last_claim_reported_at",
        "second_claim_count",
        "claim_date_tercile",
        "claimed_amount_quintile",
        "last_known_approved_amount",
    )
    .orderBy(F.col("id").desc(), "last_claim_reported_at")
)
