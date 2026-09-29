from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_status_by_claim_d = Window.partitionBy("claim_type", "status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_status_by_claim_d_2 = Window.partitionBy("claim_type", "status").orderBy("claim_date", "id")
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy("claim_date")
w_policy_id_claim_date_month_b_2 = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id")
w_claim_type_status_by_claim_d_3 = Window.partitionBy("claim_type", "status").orderBy("claim_date")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_policy_id_claim_date_month_b_3 = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").desc(), "id")
w_claim_date_quartile_prev3_cl = Window.partitionBy("claim_date_quartile", F.year("prev3_claim_date")).orderBy("claim_date_rank", "id")
w_rn_claim_date_quartile_by_cl = Window.partitionBy("rn", "claim_date_quartile").orderBy(F.col("claim_date_decile").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_claim_date_quartile_prev3_cl_2 = Window.partitionBy("claim_date_quartile", F.year("prev3_claim_date")).orderBy("claim_date_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_quartile_prev3_cl_3 = Window.partitionBy("claim_date_quartile", F.year("prev3_claim_date")).orderBy("claim_date_rank")
w_rn_claim_date_quartile_by_cl_2 = Window.partitionBy("rn", "claim_date_quartile").orderBy(F.col("claim_date_decile").asc_nulls_last(), "id")

prepared_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 13635.1) & (~F.col("status").isin("OPEN", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

ranked = (
    prepared_claims
    .select(
        "*",
        F.last("claim_date", ignorenulls=True).over(w_claim_type_status_by_claim_d).alias("last_known_claim_date"),
        F.nth_value("claim_date", 2).over(w_policy_id_claim_date_month_b).alias("second_claim_date"),
        F.last("claim_date", ignorenulls=True).over(w_claim_date_month_adjuster_id).alias("last_known_claim_date_2"),
        F.ntile(4).over(w_claim_type_status_by_claim_d_2).alias("claim_date_quartile"),
        F.last("claim_date").over(w_all_by_claim_date_full).alias("last_claim_date"),
        F.lag("claim_date").over(w_claim_type_status_by_claim_d_2).alias("prev_claim_date"),
        F.row_number().over(w_claim_type_status_by_claim_d_2).alias("claim_date_seq"),
        F.lag("claim_date", 3).over(w_claim_type_status_by_claim_d_2).alias("prev3_claim_date"),
        F.rank().over(w_claim_type_by_claim_date_tie).alias("claim_date_rank"),
        F.last("claim_date", ignorenulls=True).over(w_policy_id_claim_date_month_b_2).alias("last_known_claim_date_3"),
        F.round(F.cume_dist().over(w_claim_date_month_adjuster_id_2), 3).alias("claim_date_cume_dist"),
        F.round(F.cume_dist().over(w_claim_type_status_by_claim_d_3), 3).alias("claim_date_cume_dist_2"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)).alias("days_since_prev_claim_date"),
        F.row_number().over(w_policy_id_claim_date_month_b_3).alias("rn"),
        F.last("claim_date").over(w_policy_id_claim_date_month_b).alias("last_claim_date_2"),
        F.ntile(10).over(w_claim_type_status_by_claim_d_2).alias("claim_date_decile"),
    )
)

leaders = (
    ranked
    .filter(F.col("claim_date_seq") <= 2)
    .drop("claim_date_seq")
)

sequenced = (
    leaders
    .select(
        "*",
        (F.col("rn") - F.lag("rn").over(w_claim_date_quartile_prev3_cl)).alias("rn_change"),
        F.round(F.avg("claim_date_decile").over(w_rn_claim_date_quartile_by_cl), 2).alias("claim_date_decile_avg_6_prior"),
        F.row_number().over(w_claim_date_quartile_prev3_cl).alias("rn_2"),
        F.first("claim_date_cume_dist_2").over(w_claim_date_quartile_prev3_cl_2).alias("first_claim_date_cume_dist_2"),
        F.round(F.cume_dist().over(w_claim_date_quartile_prev3_cl_3), 3).alias("claim_date_rank_cume_dist"),
        F.nth_value("last_claim_date_2", 2).over(w_claim_date_quartile_prev3_cl_2).alias("second_last_claim_date_2"),
        (F.col("days_since_prev_claim_date") - F.lag("days_since_prev_claim_date").over(w_rn_claim_date_quartile_by_cl_2)).alias("days_since_prev_claim_date_change"),
        F.ntile(3).over(w_claim_date_quartile_prev3_cl).alias("claim_date_rank_tercile"),
    )
)

result = (
    sequenced
    .filter(F.col("claim_date_rank_cume_dist") <= 0.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "claim_type_clean",
        "second_claim_date",
        "claim_date_quartile",
        "last_claim_date",
        "prev_claim_date",
        "prev3_claim_date",
        "claim_date_rank",
        "last_known_claim_date_3",
        "claim_date_cume_dist",
        "claim_date_cume_dist_2",
        "days_since_prev_claim_date",
        "claim_date_decile",
        "rn_change",
        "claim_date_decile_avg_6_prior",
        "rn_2",
        "first_claim_date_cume_dist_2",
        "claim_date_rank_cume_dist",
        "second_last_claim_date_2",
        "claim_date_rank_tercile",
        F.initcap(F.trim(F.col("claim_type_clean"))).alias("claim_type_clean_clean"),
        F.length(F.trim(F.col("claim_type_clean"))).alias("claim_type_clean_len"),
    )
    .orderBy(
        F.col("rn_2").asc_nulls_last(),
        F.col("days_since_prev_claim_date").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
