from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc())
w_policy_id_by_claim_date_full = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").asc_nulls_last())

prepared = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

joined_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.status_claim_type_label",
        F.col("a.team").alias("adjuster_team"),
    )
)

windowed = (
    joined_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "status_claim_type_label",
        F.round(F.cume_dist().over(w_all_by_claim_date), 3).alias("claim_date_cume_dist"),
        F.dense_rank().over(w_policy_id_by_claim_date_ties).alias("claim_date_dense_rank"),
        F.nth_value("claim_date", 2).over(w_policy_id_by_claim_date_full).alias("second_claim_date"),
        F.round(F.percent_rank().over(w_all_by_claim_date_ties), 3).alias("claim_date_percent_rank"),
    )
)

result = (
    windowed
    .groupingSets(
        [["claim_date_dense_rank", "claim_type"], ["claim_date_dense_rank"], ["claim_type"], []],
        "claim_date_dense_rank", "claim_type",
    )
    .agg(
        F.grouping("claim_date_dense_rank").alias("is_total_claim_date_dense_rank"),
        F.max("claim_date_cume_dist").alias("max_claim_date_cume_dist"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(
        F.col("claim_date_dense_rank").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
