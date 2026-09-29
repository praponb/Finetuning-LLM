from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claim_date_run = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_adjuster = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("adjuster_level").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_adjuster_leve = Window.partitionBy("adjuster_id").orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_id_by_adjuster_leve_2 = Window.partitionBy("adjuster_id").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_date_month_by_adjuster_2 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("adjuster_level").desc())
w_claim_type_by_claim_date_ful = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level = Window.partitionBy("adjuster_level")
w_claim_date_month_by_adjuster_3 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_adjuster_leve_3 = Window.partitionBy("adjuster_id").orderBy(F.col("adjuster_level").desc())

prepared_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 5) & (F.month(F.col("reported_at")).isin(1, 2)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
    )
)

with_parent = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.last("claim_date", ignorenulls=True).over(w_claim_type_by_claim_date_run).alias("last_known_claim_date"),
        F.lead("claim_date", 2).over(w_claim_date_month_by_adjuster).alias("next2_claim_date"),
        F.round(F.avg("adjuster_level").over(w_policy_id), 2).alias("policy_id_avg_adjuster_level"),
        F.dense_rank().over(w_adjuster_id_by_adjuster_leve).alias("adjuster_level_dense_rank"),
        F.max("adjuster_level").over(w_adjuster_id_by_adjuster_leve_2).alias("adjuster_level_max_3"),
        F.round(F.cume_dist().over(w_claim_date_month_by_adjuster_2), 3).alias("adjuster_level_cume_dist"),
        F.first("adjuster_level").over(w_claim_type_by_claim_date_ful).alias("first_adjuster_level"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_policy_id), F.lit(0)), 4).alias("adjuster_level_share_of_policy_id"),
        F.size(F.collect_set("status").over(w_adjuster_level)).alias("distinct_status_in_adjuster_level"),
        F.round(F.avg("adjuster_level").over(w_claim_date_month_by_adjuster_3), 2).alias("adjuster_level_avg_4"),
        F.max("adjuster_level").over(w_adjuster_level).alias("adjuster_level_max_adjuster_level"),
        F.nth_value("adjuster_level", 2).over(w_all_by_claim_date_full).alias("second_adjuster_level"),
        F.dense_rank().over(w_adjuster_id_by_adjuster_leve_3).alias("adjuster_level_dense_rank_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "claim_date",
        "claim_type",
        "status",
        "adjuster_team",
        "adjuster_level",
        "last_known_claim_date",
        "next2_claim_date",
        "adjuster_level_dense_rank",
        "adjuster_level_max_3",
        "adjuster_level_cume_dist",
        "first_adjuster_level",
        "adjuster_level_share_of_policy_id",
        "second_adjuster_level",
        "adjuster_level_dense_rank_2",
    )
    .orderBy(
        F.col("adjuster_level_share_of_policy_id").desc(),
        F.col("adjuster_level_dense_rank_2").desc(),
        "id",
    )
    .limit(20)
)
