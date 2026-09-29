from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy("claim_date", "id")
w_policy_id_claim_type_by_clai_2 = Window.partitionBy("policy_id", "claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type_by_clai_3 = Window.partitionBy("policy_id", "claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_all_by_min_claim_date_dense = Window.orderBy("min_claim_date_dense_rank", "adjuster_id")
w_all_by_adjuster_level_runnin = Window.orderBy("adjuster_level", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("adjuster_team").orderBy("min_days_since_prev_claim_date", "adjuster_id")

scoped = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

filtered = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Travel") & (F.col("a.level").between(2, 3)), "left_semi")
)

metrics = (
    filtered
    .select(
        "*",
        F.lag("claim_date").over(w_policy_id_claim_type_by_clai).alias("prev_claim_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_claim_type_by_clai)).alias("days_since_prev_claim_date"),
        F.last("claim_date", ignorenulls=True).over(w_policy_id_claim_type_by_clai_2).alias("last_known_claim_date"),
        F.last("claim_date").over(w_policy_id_claim_type_by_clai_3).alias("last_claim_date"),
        F.first("claim_date").over(w_policy_id_claim_type_by_clai_3).alias("first_claim_date"),
        F.dense_rank().over(w_policy_id_claim_type_by_clai).alias("claim_date_dense_rank"),
    )
)

stats = (
    metrics
    .filter(F.col("status") == "PAID")
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date_dense_rank").alias("min_claim_date_dense_rank"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.min("days_since_prev_claim_date").alias("min_days_since_prev_claim_date"),
        F.round(100 * F.count(F.when(F.col("claim_type") == "theft", 1)) / F.count("*"), 1).alias("pct_claim_type_theft"),
        F.countDistinct("status").alias("distinct_status"),
        F.min("first_claim_date").alias("first_first_claim_date"),
        F.max("days_since_prev_claim_date").alias("max_days_since_prev_claim_date_check"),
    )
    .filter(F.col("max_days_since_prev_claim_date_check") > 30)
    .drop("max_days_since_prev_claim_date_check")
)

with_details = (
    stats.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.adjuster_id",
        "s.min_claim_date_dense_rank",
        "s.claim_type_list",
        "s.min_days_since_prev_claim_date",
        "s.first_first_claim_date",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_level), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_level"),
        F.round(F.percent_rank().over(w_all_by_min_claim_date_dense), 3).alias("min_claim_date_dense_rank_percent_rank"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_level), 2).alias("adjuster_level_vs_adjuster_level_avg"),
        F.max("adjuster_level").over(w_adjuster_level).alias("adjuster_level_max_adjuster_level"),
        F.size(F.collect_set("adjuster_id").over(w_adjuster_level)).alias("distinct_adjuster_id_in_adjuster_level"),
        F.last("first_first_claim_date", ignorenulls=True).over(w_all_by_adjuster_level_runnin).alias("last_known_first_first_claim_date"),
    )
)

latest = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "adjuster_id",
        "min_claim_date_dense_rank",
        "claim_type_list",
        "min_days_since_prev_claim_date",
        "first_first_claim_date",
        "adjuster_name",
        "adjuster_level",
        "adjuster_team",
        "min_claim_date_dense_rank_percent_rank",
        "adjuster_level_vs_adjuster_level_avg",
        "adjuster_level_max_adjuster_level",
        "distinct_adjuster_id_in_adjuster_level",
        "last_known_first_first_claim_date",
        F.when(F.col("adjuster_level") >= 3, "HIGH").when(F.col("adjuster_level") >= 1, "MEDIUM").otherwise("LOW").alias("adjuster_level_tier"),
    )
    .orderBy(F.col("last_known_first_first_claim_date").desc())
)
