from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level = Window.partitionBy("adjuster_level")
w_status_by_reported_at_runnin = Window.partitionBy("status").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level_status_by_adj = Window.partitionBy("adjuster_level", "status").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-2, -1)
w_all_by_adjuster_level = Window.orderBy("adjuster_level", "id")
w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, -1)
w_status_by_reported_at = Window.partitionBy("status").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_status_len_status = Window.partitionBy("status_len", "status")
w_adjuster_id_status_by_status = Window.partitionBy("adjuster_id", "status").orderBy("status_len", "id")
w_status_len_adjuster_id_by_ne = Window.partitionBy("status_len", "adjuster_id").orderBy("next_approved_amount_distance", "id")
w_all_by_row_count = Window.orderBy("row_count", "adjuster_id", "adjuster_level")
w_all_by_row_count_prior_6 = Window.orderBy("row_count", "adjuster_id", "adjuster_level").rowsBetween(-6, -1)

prepared_claims = (
    df_claims
    .filter((F.col("status") == "APPROVED") & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_parent = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.max("claimed_amount").over(w_adjuster_level).alias("adjuster_level_max_claimed_amount"),
        F.first("approved_amount", ignorenulls=True).over(w_status_by_reported_at_runnin).alias("first_known_approved_amount"),
        F.round(F.avg("claimed_amount").over(w_adjuster_level_status_by_adj), 2).alias("claimed_amount_avg_2_prior"),
        F.lead("approved_amount").over(w_all_by_adjuster_level).alias("next_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_level), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_reported_at_to_date_adjuster), 2).alias("adjuster_level_avg_2_prior"),
        F.lead("claimed_amount", 2).over(w_status_by_reported_at).alias("next2_claimed_amount"),
        F.last("approved_amount", ignorenulls=True).over(w_policy_id_by_approved_amount).alias("last_known_approved_amount"),
        F.dense_rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_dense_rank"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "adjuster_level",
        "first_known_approved_amount",
        "next_approved_amount",
        "claimed_amount_share_of_adjuster_level",
        "adjuster_level_avg_2_prior",
        "next2_claimed_amount",
        "last_known_approved_amount",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.abs(F.col("next_approved_amount") - 4554.8).alias("next_approved_amount_distance"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.max("approved_amount").over(w_status_len_status).alias("status_len_status_max_approved_amount"),
        F.round(F.avg("status_len").over(w_status_len_status), 2).alias("status_len_status_avg_status_len"),
        F.lead("status_len", 3).over(w_adjuster_id_status_by_status).alias("next3_status_len"),
        F.round(F.percent_rank().over(w_status_len_adjuster_id_by_ne), 3).alias("next_approved_amount_distance_percent_rank"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("claimed_amount_share_of_adjuster_level") >= 0.2)
    .groupBy("adjuster_id", "adjuster_level")
    .agg(
        F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("n_status_rejected"),
        F.count("*").alias("row_count"),
    )
)

sequenced = (
    rollup_base
    .withColumn("prev_row_count", F.lag("row_count", 1, 0).over(w_all_by_row_count))
    .withColumn("adjuster_level_avg_6_prior", F.round(F.avg("adjuster_level").over(w_all_by_row_count_prior_6), 2))
)

result = (
    sequenced
    .select(
        "adjuster_id",
        "adjuster_level",
        "row_count",
        "prev_row_count",
        "adjuster_level_avg_6_prior",
        F.abs(F.col("adjuster_level") - 3).alias("adjuster_level_distance"),
    )
)
