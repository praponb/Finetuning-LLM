from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status = Window.partitionBy("policy_status")
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claim_date").desc())
w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_level_claim_type_by = Window.partitionBy("adjuster_level", "claim_type").orderBy(F.col("claim_date").desc_nulls_first())
w_all_by_adjuster_level_ties = Window.orderBy("adjuster_level")
w_all_by_adjuster_level_ties_2 = Window.orderBy(F.col("adjuster_level").desc())
w_claim_date_year_adjuster_id_2 = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type_by_appr_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-1, Window.currentRow)

scoped_claims = df_claims.filter(F.col("approved_amount") <= 34401.0)

joined = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        F.col("p.status").alias("policy_status"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_status), F.lit(0)), 4).alias("adjuster_level_share_of_policy_status"),
        F.round(F.cume_dist().over(w_claim_date_year_adjuster_id), 3).alias("claim_date_cume_dist"),
        F.row_number().over(w_all_by_adjuster_level).alias("seq_no"),
        F.round(F.cume_dist().over(w_adjuster_level_claim_type_by), 3).alias("claim_date_cume_dist_2"),
        F.dense_rank().over(w_all_by_adjuster_level_ties).alias("adjuster_level_dense_rank"),
        F.rank().over(w_all_by_adjuster_level_ties_2).alias("adjuster_level_rank"),
        F.first("approved_amount").over(w_claim_date_year_adjuster_id_2).alias("first_approved_amount"),
        F.sum("adjuster_level").over(w_policy_id_claim_type_by_appr).alias("running_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_policy_id_claim_type_by_appr_2), 2).alias("adjuster_level_avg_2"),
    )
)

scored = (
    with_trends
    .filter(F.col("approved_amount") > 0.0)
    .withColumn("running_adjuster_level_distance", F.abs(F.col("running_adjuster_level") - 33))
)

result = (
    scored
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "policy_status",
        "adjuster_level",
        "adjuster_name",
        "adjuster_level_share_of_policy_status",
        "claim_date_cume_dist",
        "claim_date_cume_dist_2",
        "adjuster_level_rank",
        "running_adjuster_level",
        "adjuster_level_avg_2",
    )
    .orderBy("adjuster_level_avg_2")
)
