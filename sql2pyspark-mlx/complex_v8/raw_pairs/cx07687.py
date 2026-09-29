from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_reported_at_dayo = Window.partitionBy("adjuster_id", "reported_at_dayofweek").orderBy(F.col("reported_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_reported_at_month = Window.partitionBy("claim_type", F.month("reported_at")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(-1, -1)
w_all_by_reported_at_dayofweek = Window.orderBy(F.col("reported_at_dayofweek").asc_nulls_last(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc())
w_distinct_policy_id_in_claim = Window.partitionBy("distinct_policy_id_in_claim_type", "claim_type").orderBy("adjuster_id_sum_claimed_amount", "id")
w_reported_at_dayofweek_seq = Window.partitionBy("reported_at_dayofweek_seq")
w_n_claim_type_flood_distinct = Window.partitionBy("n_claim_type_flood", "distinct_policy_id_in_claim_type")
w_distinct_policy_id_in_claim_2 = Window.partitionBy("distinct_policy_id_in_claim_type", "n_claim_type_flood").orderBy(F.col("n_claim_type_flood").asc_nulls_last(), "distinct_policy_id_in_claim_type", "second_claim_date_ym", "reported_at_dayofweek_seq").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_policy_id_in_claim_3 = Window.partitionBy("distinct_policy_id_in_claim_type", "n_claim_type_flood").orderBy(F.col("n_claim_type_flood").asc_nulls_last(), "distinct_policy_id_in_claim_type", "second_claim_date_ym", "reported_at_dayofweek_seq")
w_distinct_policy_id_in_claim_4 = Window.partitionBy("distinct_policy_id_in_claim_type", "n_claim_type_flood").orderBy(F.col("n_claim_type_flood").asc_nulls_last(), "distinct_policy_id_in_claim_type", "second_claim_date_ym", "reported_at_dayofweek_seq").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_claims = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("policy_id") >= 16))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
    )
)

sequenced = (
    valid_claims
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.min("claimed_amount").over(w_claim_type).alias("claim_type_min_claimed_amount"),
        F.nth_value("claim_date", 2).over(w_adjuster_id_reported_at_dayo).alias("second_claim_date"),
        F.sum("reported_at_dayofweek").over(w_claim_type_reported_at_month).alias("reported_at_dayofweek_sum_1_prior"),
        F.min("reported_at_dayofweek").over(w_claim_type).alias("claim_type_min_reported_at_dayofweek"),
        F.row_number().over(w_all_by_reported_at_dayofweek).alias("reported_at_dayofweek_seq"),
        F.sum("claimed_amount").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount"),
        F.sum("claimed_amount").over(w_all_by_reported_at_running).alias("running_claimed_amount"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "reported_at_dayofweek",
        "distinct_policy_id_in_claim_type",
        "second_claim_date",
        "reported_at_dayofweek_sum_1_prior",
        "claim_type_min_reported_at_dayofweek",
        "reported_at_dayofweek_seq",
        "adjuster_id_sum_claimed_amount",
        "running_claimed_amount",
        F.rank().over(w_claim_type_by_claim_date_tie).alias("claim_date_rank"),
        F.round(F.cume_dist().over(w_claim_type_by_claim_date_tie), 3).alias("claim_date_cume_dist"),
        (F.col("reported_at_dayofweek") - F.lag("reported_at_dayofweek").over(w_distinct_policy_id_in_claim)).alias("reported_at_dayofweek_change"),
        F.max("claimed_amount").over(w_reported_at_dayofweek_seq).alias("reported_at_dayofweek_seq_max_claimed_amount"),
    )
)

per_group = (
    sequenced_2
    .groupBy(
        "distinct_policy_id_in_claim_type",
        F.date_format("second_claim_date", "yyyy-MM").alias("second_claim_date_ym"),
        "reported_at_dayofweek_seq",
    )
    .agg(
        F.sum(F.when(F.col("claim_type") == "flood", 1).otherwise(0)).alias("n_claim_type_flood"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

windowed = (
    per_group
    .select(
        "distinct_policy_id_in_claim_type",
        "second_claim_date_ym",
        "reported_at_dayofweek_seq",
        "n_claim_type_flood",
        "distinct_ids",
        F.sum("distinct_ids").over(w_n_claim_type_flood_distinct).alias("n_claim_type_flood_distinct_policy_id_in_claim_t"),
        F.first("n_claim_type_flood", ignorenulls=True).over(w_distinct_policy_id_in_claim_2).alias("first_known_n_claim_type_flood"),
        F.round(F.cume_dist().over(w_distinct_policy_id_in_claim_3), 3).alias("n_claim_type_flood_cume_dist"),
        F.nth_value("distinct_ids", 2).over(w_distinct_policy_id_in_claim_4).alias("second_distinct_ids"),
    )
)

result = (
    windowed
    .select(
        "distinct_policy_id_in_claim_type",
        "reported_at_dayofweek_seq",
        "n_claim_type_flood",
        "n_claim_type_flood_distinct_policy_id_in_claim_t",
        "first_known_n_claim_type_flood",
        "n_claim_type_flood_cume_dist",
        "second_distinct_ids",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
    )
    .orderBy(
        F.col("n_claim_type_flood").desc(),
        F.col("distinct_policy_id_in_claim_type").desc(),
    )
)
