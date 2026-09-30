from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_adjuster_id_b = Window.partitionBy("policy_product", "adjuster_id").orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_policy_product_by_approved_a = Window.partitionBy("policy_product").orderBy(F.col("approved_amount").desc())
w_policy_product_by_approved_a_2 = Window.partitionBy("policy_product").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_product_adjuster_id_b_2 = Window.partitionBy("policy_product", "adjuster_id").orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").asc_nulls_last())
w_policy_product_adjuster_id_b_3 = Window.partitionBy("policy_product", "adjuster_id").orderBy("approved_amount_adj", "id").rowsBetween(-2, -1)
w_policy_product_adjuster_id_b_4 = Window.partitionBy("policy_product", "adjuster_id").orderBy("approved_amount_adj")
w_distinct_claim_types_distinc = Window.partitionBy("distinct_claim_types", "distinct_adjuster_ids")
w_distinct_adjuster_ids_rn_by = Window.partitionBy("distinct_adjuster_ids", "rn").orderBy(F.col("max_approved_amount_seq").asc_nulls_last(), "reported_at_day", "rn").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_claim_types = Window.orderBy("distinct_claim_types", "reported_at_day", "rn")

base = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
    )
)

with_details = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.product").alias("policy_product"))
)

windowed = (
    with_details
    .select(
        "*",
        F.round(F.avg("approved_amount_adj").over(w_policy_product_adjuster_id_b), 2).alias("approved_amount_adj_avg_3"),
        F.round(F.cume_dist().over(w_policy_product_by_approved_a), 3).alias("approved_amount_cume_dist"),
        F.first("claim_date", ignorenulls=True).over(w_policy_product_by_approved_a_2).alias("first_known_claim_date"),
        F.row_number().over(w_policy_product_adjuster_id_b_2).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.dense_rank().over(w_all_by_approved_amount_ties).alias("approved_amount_dense_rank"),
        F.max("approved_amount_adj").over(w_policy_product_adjuster_id_b_3).alias("approved_amount_adj_max_2_prior"),
        F.dense_rank().over(w_policy_product_adjuster_id_b_4).alias("approved_amount_adj_dense_rank"),
        F.row_number().over(w_all_by_approved_amount).alias("approved_amount_seq"),
    )
)

derived = (
    windowed
    .select(
        "*",
        F.round(F.col("approved_amount") * 1.07, 2).alias("approved_amount_adj_2"),
        F.least(F.col("rn"), F.col("approved_amount")).alias("least_rn_approved_amount"),
    )
)

per_group = (
    derived
    .groupBy(F.to_date("reported_at").alias("reported_at_day"), "rn")
    .agg(
        F.round(F.stddev("approved_amount_adj"), 2).alias("stddev_approved_amount_adj"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.max("approved_amount_seq").alias("max_approved_amount_seq"),
        F.round(F.avg("approved_amount_percent_rank"), 2).alias("avg_approved_amount_percent_rank"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.avg("approved_amount_seq").alias("avg_approved_amount_seq_check"),
    )
    .filter(F.col("avg_approved_amount_seq_check") > 3)
    .drop("avg_approved_amount_seq_check")
)

metrics = (
    per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_claim_types_distinc).alias("rows_in_distinct_claim_types_distinct_adjuster_i"),
        F.nth_value("stddev_approved_amount_adj", 2).over(w_distinct_adjuster_ids_rn_by).alias("second_stddev_approved_amount_adj"),
        (F.col("distinct_claim_types") - F.lag("distinct_claim_types").over(w_all_by_distinct_claim_types)).alias("distinct_claim_types_change"),
    )
)

result = (
    metrics
    .select(
        "reported_at_day",
        "rn",
        "stddev_approved_amount_adj",
        "distinct_adjuster_ids",
        "max_approved_amount_seq",
        "distinct_claim_types",
        "rows_in_distinct_claim_types_distinct_adjuster_i",
        "second_stddev_approved_amount_adj",
        "distinct_claim_types_change",
        (F.floor(F.col("distinct_claim_types") / 2) * 2).alias("distinct_claim_types_bucket"),
    )
    .orderBy(F.col("rn").asc())
)
