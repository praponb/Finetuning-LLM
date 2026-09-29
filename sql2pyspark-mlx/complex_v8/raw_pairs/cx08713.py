from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_prior_1 = Window.orderBy("reported_at", "id").rowsBetween(-1, -1)
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy("reported_at", "id")
w_policy_id_policy_product = Window.partitionBy("policy_id", "policy_product")
w_policy_id_adjuster_level = Window.partitionBy("policy_id", "adjuster_level")
w_policy_id_by_policy_end_date = Window.partitionBy("policy_id").orderBy("policy_end_date", "id")
w_policy_id_by_reported_at_run = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_adjuster_level_runnin = Window.orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_adjuster_level = Window.orderBy("adjuster_level", "id")
w_policy_id_by_policy_end_date_2 = Window.partitionBy("policy_id").orderBy("policy_end_date")
w_all_by_approved_amount_avg_1 = Window.orderBy("approved_amount_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_end_date_dense_rank_b = Window.partitionBy("policy_end_date_dense_rank").orderBy("adjuster_level_vs_policy_id_policy_product_avg", "id")
w_first_known_adjuster_level_p = Window.partitionBy("first_known_adjuster_level", "policy_end_date_dense_rank")

cleaned = (
    df_claims
    .filter(F.col("policy_id") <= 13)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        "adjuster_name",
        "policy_end_date",
        F.round(F.avg("approved_amount").over(w_all_by_reported_at_prior_1), 2).alias("approved_amount_avg_1_prior"),
        F.dense_rank().over(w_policy_id_by_reported_at).alias("reported_at_dense_rank"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_id_policy_product), 2).alias("adjuster_level_vs_policy_id_policy_product_avg"),
        F.round(F.avg("adjuster_level").over(w_policy_id_adjuster_level), 2).alias("policy_id_adjuster_level_avg_adjuster_level"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_id_adjuster_level), F.lit(0)), 4).alias("adjuster_level_share_of_policy_id_adjuster_level"),
        F.dense_rank().over(w_policy_id_by_policy_end_date).alias("policy_end_date_dense_rank"),
        F.first("policy_end_date", ignorenulls=True).over(w_policy_id_by_reported_at_run).alias("first_known_policy_end_date"),
        F.first("adjuster_level", ignorenulls=True).over(w_all_by_adjuster_level_runnin).alias("first_known_adjuster_level"),
        F.datediff(F.col("policy_end_date"), F.lag("policy_end_date").over(w_all_by_adjuster_level)).alias("days_since_prev_policy_end_date"),
        F.round(F.percent_rank().over(w_policy_id_by_policy_end_date_2), 3).alias("policy_end_date_percent_rank"),
        F.round(F.cume_dist().over(w_policy_id_by_policy_end_date_2), 3).alias("policy_end_date_cume_dist"),
        F.sum("approved_amount").over(w_policy_id_adjuster_level).alias("policy_id_adjuster_level_sum_approved_amount"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.first("first_known_adjuster_level").over(w_all_by_approved_amount_avg_1).alias("first_first_known_adjuster_level"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_end_date_dense_rank_b)).alias("approved_amount_change"),
        F.size(F.collect_set("policy_id").over(w_first_known_adjuster_level_p)).alias("distinct_policy_id_in_first_known_adjuster_level"),
        F.size(F.collect_set("adjuster_id").over(w_first_known_adjuster_level_p)).alias("distinct_adjuster_id_in_first_known_adjuster_lev"),
        F.count(F.lit(1)).over(w_first_known_adjuster_level_p).alias("rows_in_first_known_adjuster_level_policy_end_da"),
        F.lag("policy_end_date", 3).over(w_policy_end_date_dense_rank_b).alias("prev3_policy_end_date"),
        F.row_number().over(w_policy_end_date_dense_rank_b).alias("rn"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "reported_at",
        "adjuster_name",
        "policy_end_date",
        "approved_amount_avg_1_prior",
        "reported_at_dense_rank",
        "adjuster_level_vs_policy_id_policy_product_avg",
        "policy_id_adjuster_level_avg_adjuster_level",
        "first_known_policy_end_date",
        "first_known_adjuster_level",
        "days_since_prev_policy_end_date",
        "policy_end_date_percent_rank",
        "policy_end_date_cume_dist",
        "first_first_known_adjuster_level",
        "distinct_adjuster_id_in_first_known_adjuster_lev",
        "prev3_policy_end_date",
        "rn",
        F.least(F.col("approved_amount"), F.col("rn")).alias("least_approved_amount_rn"),
    )
    .orderBy(F.col("policy_id").desc_nulls_first())
)
