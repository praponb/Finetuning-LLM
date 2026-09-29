from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claim_date", "id")
w_claim_date_year_policy_id_by = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id")
w_claim_date_year_policy_id_by_2 = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id_claim_type_by_cl = Window.partitionBy("adjuster_id", "claim_type").orderBy("claimed_amount", "id").rowsBetween(-3, Window.currentRow)
w_all_by_claim_date_moving_1 = Window.orderBy("claim_date", "id").rowsBetween(-1, Window.currentRow)
w_adjuster_id_claim_type_by_cl_2 = Window.partitionBy("adjuster_id", "claim_type").orderBy("claimed_amount", "id")
w_claim_type_adjuster_id_by_cl_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .filter((F.col("approved_amount") > 0.0) | (F.col("claimed_amount") < 40639.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "status_prefix",
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
        F.ntile(3).over(w_claim_type_adjuster_id_by_cl).alias("claim_date_tercile"),
        F.max("claimed_amount").over(w_claim_date_year_policy_id_by).alias("max_claimed_amount_so_far"),
        F.lag("claim_date", 2).over(w_claim_type_adjuster_id_by_cl).alias("prev2_claim_date"),
        F.first("claim_date", ignorenulls=True).over(w_claim_date_year_policy_id_by).alias("first_known_claim_date"),
        F.size(F.collect_set("claim_type").over(w_claim_date_year_adjuster_id)).alias("distinct_claim_type_in_claim_date_year_adjuster_"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_year_adjuster_id), 2).alias("claimed_amount_vs_claim_date_year_adjuster_id_av"),
        F.ntile(5).over(w_claim_date_year_policy_id_by_2).alias("claim_date_quintile"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_claim_type_by_cl), 2).alias("claimed_amount_avg_4"),
        F.sum("claimed_amount").over(w_all_by_claim_date_moving_1).alias("claimed_amount_sum_2"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.row_number().over(w_claim_date_year_policy_id_by_2).alias("seq_no"),
        F.lead("claim_date").over(w_claim_date_year_policy_id_by_2).alias("next_claim_date"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_adjuster_id_claim_type_by_cl_2)).alias("claimed_amount_change"),
        F.first("claimed_amount").over(w_claim_type_adjuster_id_by_cl_2).alias("first_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_year_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_year_adjuster"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "claim_date",
        "policy_id_sum_claimed_amount",
        "claim_date_tercile",
        "max_claimed_amount_so_far",
        "prev2_claim_date",
        "distinct_claim_type_in_claim_date_year_adjuster_",
        "claim_date_quintile",
        "claimed_amount_avg_4",
        "claimed_amount_share_of_policy_id",
        "seq_no",
        "next_claim_date",
        "claimed_amount_change",
        F.datediff(F.col("prev2_claim_date"), F.col("first_known_claim_date")).alias("days_first_known_claim_date_to_prev2_claim_date"),
        F.abs(F.col("claim_date_tercile") - 7).alias("claim_date_tercile_distance"),
    )
)

result = (
    scored
    .filter(F.col("days_first_known_claim_date_to_prev2_claim_date") < 420)
    .select(
        "id",
        "claim_date",
        "policy_id_sum_claimed_amount",
        "claim_date_tercile",
        "max_claimed_amount_so_far",
        "prev2_claim_date",
        "distinct_claim_type_in_claim_date_year_adjuster_",
        "claim_date_quintile",
        "claimed_amount_avg_4",
        "claimed_amount_share_of_policy_id",
        "seq_no",
        "next_claim_date",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy(F.col("claim_date_quarter").desc(), F.col("seq_no").desc())
)
