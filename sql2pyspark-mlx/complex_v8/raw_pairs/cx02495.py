from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_days = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("days_since_reported_at").desc())
w_all_by_claim_date_moving_1 = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_policy_id_claim_type_by_days_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("days_since_reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_claim_type_by_days_3 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("days_since_reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_claim_type_by_days_since_rep = Window.partitionBy("claim_type").orderBy("days_since_reported_at")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_claim_type_by_days_since_rep_2 = Window.partitionBy("claim_type").orderBy("days_since_reported_at", "id")
w_claim_type_by_days_since_rep_3 = Window.partitionBy("claim_type").orderBy("days_since_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_days_since_reported_at_dense = Window.partitionBy("days_since_reported_at_dense_rank")
w_claim_type_by_days_since_rep_4 = Window.partitionBy("claim_type").orderBy(F.col("days_since_reported_at_dense_rank").desc(), "id")
w_all_by_days_since_reported_a = Window.orderBy(F.col("days_since_reported_at_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_reported_a_2 = Window.orderBy("days_since_reported_at_max_2")
w_claim_type_by_days_since_rep_5 = Window.partitionBy("claim_type").orderBy(F.col("days_since_reported_at_dense_rank").desc())

cleaned_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

joined_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Motor", "Property")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.days_since_reported_at",
        "a.adjuster_name",
    )
)

sequenced = (
    joined_adjuster
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_id_claim_type_by_days), 3).alias("days_since_reported_at_cume_dist"),
        F.max("days_since_reported_at").over(w_all_by_claim_date_moving_1).alias("days_since_reported_at_max_2"),
        F.nth_value("days_since_reported_at", 2).over(w_policy_id_claim_type_by_days_2).alias("second_days_since_reported_at"),
        F.sum("days_since_reported_at").over(w_adjuster_id).alias("adjuster_id_sum_days_since_reported_at"),
        F.round(F.avg("days_since_reported_at").over(w_adjuster_id), 2).alias("adjuster_id_avg_days_since_reported_at"),
        F.first("claim_date", ignorenulls=True).over(w_policy_id_claim_type_by_days_3).alias("first_known_claim_date"),
        F.round(F.col("days_since_reported_at") - F.avg("days_since_reported_at").over(w_adjuster_id), 2).alias("days_since_reported_at_vs_adjuster_id_avg"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)).alias("days_since_prev_claim_date"),
        F.dense_rank().over(w_claim_type_by_days_since_rep).alias("days_since_reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_policy_id_claim_type_by_days_3).alias("running_count"),
        F.round(F.percent_rank().over(w_all_by_claim_date_ties), 3).alias("claim_date_percent_rank"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.lead("claim_date", 2).over(w_claim_type_by_days_since_rep_2).alias("next2_claim_date"),
        F.first("claim_date", ignorenulls=True).over(w_claim_type_by_days_since_rep_3).alias("first_known_claim_date_2"),
    )
)

tagged = (
    sequenced
    .withColumn("claim_date_percent_rank_adj", F.round(F.col("claim_date_percent_rank") * 0.25, 2))
)

sequenced_2 = (
    tagged
    .select(
        "id",
        "claim_date",
        "days_since_reported_at",
        "adjuster_name",
        "days_since_reported_at_cume_dist",
        "days_since_reported_at_max_2",
        "second_days_since_reported_at",
        "adjuster_id_avg_days_since_reported_at",
        "first_known_claim_date",
        "days_since_reported_at_vs_adjuster_id_avg",
        "days_since_prev_claim_date",
        "days_since_reported_at_dense_rank",
        "running_count",
        "claim_date_percent_rank",
        "claim_date_percent_rank_adj",
        F.round(F.avg("days_since_prev_claim_date").over(w_days_since_reported_at_dense), 2).alias("days_since_reported_at_dense_rank_avg_days_since"),
        F.round(F.col("days_since_reported_at_max_2") - F.avg("days_since_reported_at_max_2").over(w_days_since_reported_at_dense), 2).alias("days_since_reported_at_max_2_vs_days_since_repor"),
        F.round(F.cume_dist().over(w_claim_type_by_days_since_rep_4), 3).alias("days_since_reported_at_dense_rank_cume_dist"),
        F.first("first_known_claim_date_2", ignorenulls=True).over(w_all_by_days_since_reported_a).alias("first_known_first_known_claim_date_2"),
        F.last("days_since_prev_claim_date", ignorenulls=True).over(w_all_by_days_since_reported_a).alias("last_known_days_since_prev_claim_date"),
        F.round(F.cume_dist().over(w_all_by_days_since_reported_a_2), 3).alias("days_since_reported_at_max_2_cume_dist"),
        F.round(F.percent_rank().over(w_claim_type_by_days_since_rep_5), 3).alias("days_since_reported_at_dense_rank_percent_rank"),
    )
)

result = (
    sequenced_2
    .filter(F.col("days_since_reported_at_cume_dist") <= 0.9)
    .select(
        "id",
        "claim_date",
        "days_since_reported_at",
        "adjuster_name",
        "days_since_reported_at_cume_dist",
        "days_since_reported_at_max_2",
        "second_days_since_reported_at",
        "adjuster_id_avg_days_since_reported_at",
        "days_since_reported_at_vs_adjuster_id_avg",
        "days_since_prev_claim_date",
        "running_count",
        "claim_date_percent_rank",
        "claim_date_percent_rank_adj",
        "days_since_reported_at_dense_rank_avg_days_since",
        "days_since_reported_at_max_2_vs_days_since_repor",
        "first_known_first_known_claim_date_2",
        "last_known_days_since_prev_claim_date",
        "days_since_reported_at_max_2_cume_dist",
        "days_since_reported_at_dense_rank_percent_rank",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.coalesce(F.col("claim_date_percent_rank"), F.lit(0.0)).alias("claim_date_percent_rank_filled"),
    )
    .orderBy("days_since_reported_at_max_2")
)
