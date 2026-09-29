from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_adjuster_level_by = Window.partitionBy("policy_id", "adjuster_level").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_adjuster_id_by_adjuster_leve = Window.partitionBy("adjuster_id").orderBy("adjuster_level")
w_policy_id_adjuster_level_by_2 = Window.partitionBy("policy_id", "adjuster_level").orderBy(F.col("adjuster_level").asc_nulls_last())
w_policy_id_by_adjuster_level = Window.partitionBy("policy_id").orderBy("adjuster_level", "id")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")

scoped = (
    df_claims
    .filter((F.col("policy_id") <= 5) & (~F.col("claim_type").isin("collision", "flood")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

with_details_adjuster = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_windows = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "adjuster_name",
        "adjuster_level",
        F.dense_rank().over(w_adjuster_id_by_claimed_amoun).alias("claimed_amount_dense_rank"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_id), 2).alias("adjuster_level_vs_policy_id_avg"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_adjuster_level_by)).alias("claimed_amount_change"),
        F.rank().over(w_adjuster_id_by_adjuster_leve).alias("adjuster_level_rank"),
        F.rank().over(w_policy_id_adjuster_level_by_2).alias("adjuster_level_rank_2"),
        F.round(F.cume_dist().over(w_adjuster_id_by_adjuster_leve), 3).alias("adjuster_level_cume_dist"),
        F.round(F.cume_dist().over(w_policy_id_by_adjuster_level), 3).alias("adjuster_level_cume_dist_2"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claimed_amoun), 3).alias("claimed_amount_cume_dist"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_by_claimed_amoun_2), 2).alias("claimed_amount_avg_3"),
        F.rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_rank"),
        F.round(F.percent_rank().over(w_adjuster_id_by_adjuster_leve), 3).alias("adjuster_level_percent_rank"),
        F.lag("claimed_amount").over(w_all_by_claimed_amount).alias("prev_claimed_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "adjuster_name",
        "adjuster_level",
        "claimed_amount_dense_rank",
        "adjuster_level_rank",
        "adjuster_level_cume_dist_2",
        "claimed_amount_cume_dist",
        "claimed_amount_rank",
        "adjuster_level_percent_rank",
        "prev_claimed_amount",
    )
    .orderBy(F.col("claimed_amount_rank").desc_nulls_first())
)
