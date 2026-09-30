from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_adjuster_id_status_by_claime = Window.partitionBy("adjuster_id", "status").orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_claim_date_dayofweek = Window.orderBy("claim_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_status_by_claime_2 = Window.partitionBy("adjuster_id", "status").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_dayofweek_2 = Window.orderBy("claim_date_dayofweek")
w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_prev_claim_date = Window.partitionBy("adjuster_id", "prev_claim_date_dayofweek").orderBy(F.col("rows_in_status").desc(), "id")

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "claim_type_prefix",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.round(F.cume_dist().over(w_adjuster_id_status_by_claime), 3).alias("claimed_amount_cume_dist"),
        F.sum("claim_date_dayofweek").over(w_status).alias("status_sum_claim_date_dayofweek"),
        F.min("claimed_amount").over(w_status).alias("status_min_claimed_amount"),
        F.first("claimed_amount").over(w_all_by_claim_date_dayofweek).alias("first_claimed_amount"),
        F.lag("claim_date_dayofweek").over(w_adjuster_id_status_by_claime).alias("prev_claim_date_dayofweek"),
        F.round(F.col("claim_date_dayofweek") / F.nullif(F.sum("claim_date_dayofweek").over(w_status), F.lit(0)), 4).alias("claim_date_dayofweek_share_of_status"),
        F.nth_value("claim_date_dayofweek", 2).over(w_adjuster_id_status_by_claime_2).alias("second_claim_date_dayofweek"),
        F.first("claim_date_dayofweek").over(w_all_by_claim_date_dayofweek).alias("first_claim_date_dayofweek"),
        F.dense_rank().over(w_all_by_claim_date_dayofweek_2).alias("claim_date_dayofweek_dense_rank"),
    )
)

leaders = metrics.filter(F.col("claim_date_dayofweek_dense_rank") == 1)

windowed = (
    leaders
    .select(
        "*",
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.lead("claimed_amount", 1, 0).over(w_adjuster_id_prev_claim_date).alias("next_claimed_amount"),
        F.sum("claimed_amount").over(w_claim_type).alias("claim_type_sum_claimed_amount"),
    )
)

result = (
    windowed
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "claim_type_prefix",
        "claimed_amount_cume_dist",
        "status_sum_claim_date_dayofweek",
        "status_min_claimed_amount",
        "first_claimed_amount",
        "prev_claim_date_dayofweek",
        "second_claim_date_dayofweek",
        "first_claim_date_dayofweek",
        "claim_date_dayofweek_dense_rank",
        "distinct_status_in_claim_type",
        "next_claimed_amount",
        "claim_type_sum_claimed_amount",
    )
    .orderBy(
        F.col("distinct_status_in_claim_type").asc_nulls_last(),
        F.col("status_min_claimed_amount").asc_nulls_last(),
    )
)
