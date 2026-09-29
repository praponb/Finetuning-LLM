from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_len_policy_id_by = Window.partitionBy("claim_type_len", "policy_id").orderBy(F.col("claim_type_len").desc(), "id")
w_claim_type_len = Window.partitionBy("claim_type_len")
w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy("claim_type_len", "id")
w_claim_type_len_policy_id_by_2 = Window.partitionBy("claim_type_len", "policy_id").orderBy(F.col("claim_type_len").desc())
w_claim_type_len_adjuster_id_b = Window.partitionBy("claim_type_len", "adjuster_id").orderBy("claim_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_claim_type = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_policy_id_claim_type_by_clai_2 = Window.partitionBy("policy_id", "claim_type").orderBy("claim_type_len")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())

scoped = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claimed_amount") >= 18778.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.round(F.percent_rank().over(w_claim_type_len_policy_id_by), 3).alias("claim_type_len_percent_rank"),
        F.size(F.collect_set("policy_id").over(w_claim_type_len)).alias("distinct_policy_id_in_claim_type_len"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_policy_id_claim_type_by_clai))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.dense_rank().over(w_claim_type_len_policy_id_by_2).alias("claim_type_len_dense_rank"),
        F.max("claim_type_len").over(w_claim_type_len_adjuster_id_b).alias("max_claim_type_len_so_far"),
        F.round(F.avg("claim_type_len").over(w_claim_date_month_claim_type), 2).alias("claim_date_month_claim_type_avg_claim_type_len"),
        F.round(F.percent_rank().over(w_all_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.rank().over(w_policy_id_claim_type_by_clai_2).alias("claim_type_len_rank"),
        F.sum("claim_type_len").over(w_claim_type_len).alias("claim_type_len_sum_claim_type_len"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "claim_type_len",
        "claim_type_len_percent_rank",
        "minutes_since_prev_reported_at",
        "max_claim_type_len_so_far",
        "claim_date_month_claim_type_avg_claim_type_len",
        "claim_date_percent_rank",
        "claim_type_len_rank",
        "claim_date_cume_dist",
        F.round(F.col("claim_date_percent_rank") * 0.9, 2).alias("claim_date_percent_rank_adj"),
    )
    .orderBy(F.col("max_claim_type_len_so_far").asc())
)
