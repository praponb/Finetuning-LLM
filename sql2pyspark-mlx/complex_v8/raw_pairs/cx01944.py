from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy("approved_amount", "id")
w_claim_type_by_claim_date = Window.partitionBy("claim_type").orderBy("claim_date", "id")
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_distinct_status_in_claim_typ = Window.partitionBy("distinct_status_in_claim_type").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_seq_no_by_rows_in_claim_type = Window.partitionBy("seq_no").orderBy(F.col("rows_in_claim_type").asc_nulls_last())
w_distinct_status_in_claim_typ_2 = Window.partitionBy("distinct_status_in_claim_type").orderBy(F.col("claim_date").asc_nulls_last(), "id")

filtered_claims = df_claims.filter(F.col("policy_id") <= 23)

sequenced = (
    filtered_claims
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        F.dense_rank().over(w_all_by_claimed_amount).alias("claimed_amount_dense_rank"),
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.lag("reported_at").over(w_claim_type_by_approved_amoun).alias("prev_reported_at"),
        F.row_number().over(w_claim_type_by_claim_date).alias("seq_no"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.lag("claimed_amount", 3).over(w_claim_type_by_claim_date).alias("prev3_claimed_amount"),
        F.round(F.cume_dist().over(w_claim_type_by_approved_amoun), 3).alias("approved_amount_cume_dist"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_dense_rank",
        "distinct_status_in_claim_type",
        "approved_amount_cume_dist",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_adjuster_id"),
        F.max("approved_amount").over(w_distinct_status_in_claim_typ).alias("approved_amount_max_4"),
        F.round(F.cume_dist().over(w_seq_no_by_rows_in_claim_type), 3).alias("rows_in_claim_type_cume_dist"),
        F.lag("approved_amount_cume_dist").over(w_distinct_status_in_claim_typ_2).alias("prev_approved_amount_cume_dist"),
        F.count(F.lit(1)).over(w_policy_id_adjuster_id).alias("rows_in_policy_id_adjuster_id"),
        F.lead("approved_amount_cume_dist", 3).over(w_distinct_status_in_claim_typ_2).alias("next3_approved_amount_cume_dist"),
    )
)

result = (
    with_windows
    .filter(F.col("approved_amount_cume_dist") < 0.8)
    .select(
        "id",
        "claim_date",
        "status",
        "claimed_amount_dense_rank",
        "approved_amount_share_of_policy_id_adjuster_id",
        "approved_amount_max_4",
        "rows_in_claim_type_cume_dist",
        "prev_approved_amount_cume_dist",
        "rows_in_policy_id_adjuster_id",
        "next3_approved_amount_cume_dist",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
    .orderBy(
        F.col("approved_amount_max_4").desc(),
        F.col("rows_in_claim_type_cume_dist").asc(),
        "id",
    )
    .limit(20)
)
