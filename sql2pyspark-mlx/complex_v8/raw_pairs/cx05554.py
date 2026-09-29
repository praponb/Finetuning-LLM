from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date_by_claim = Window.partitionBy(F.to_date("reported_at")).orderBy("claim_date", "id")
w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy("claim_date")
w_reported_at_to_date_by_claim_2 = Window.partitionBy(F.to_date("reported_at")).orderBy("claim_date")
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy("claim_date", "id")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id")
w_status_by_claim_date = Window.partitionBy("status").orderBy("claim_date", "id")
w_adjuster_id_by_claim_date_fu = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_all_by_minutes_since_prev_re = Window.orderBy("minutes_since_prev_reported_at")

recent = (
    df_claims
    .filter(~F.col("status").isin("PAID", "APPROVED"))
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

windowed = (
    recent
    .select(
        "id",
        "adjuster_id",
        "status",
        "reported_at",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_reported_at_to_date_by_claim))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.percent_rank().over(w_adjuster_id_by_claim_date_ti), 3).alias("claim_date_percent_rank"),
        F.rank().over(w_adjuster_id_by_claim_date_ti).alias("claim_date_rank"),
        F.rank().over(w_reported_at_to_date_by_claim_2).alias("claim_date_rank_2"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_by_claim_date)).alias("days_since_prev_claim_date"),
        F.round(F.percent_rank().over(w_policy_id_by_claim_date), 3).alias("claim_date_percent_rank_2"),
        F.lead("claim_date").over(w_status_by_claim_date).alias("next_claim_date"),
        F.ntile(10).over(w_adjuster_id_by_claim_date).alias("claim_date_decile"),
        F.first("reported_at").over(w_adjuster_id_by_claim_date_fu).alias("first_reported_at"),
        F.row_number().over(w_adjuster_id_by_claim_date).alias("rn"),
        F.nth_value("claim_date", 2).over(w_adjuster_id_by_claim_date_fu).alias("second_claim_date"),
        F.ntile(3).over(w_adjuster_id_by_claim_date).alias("claim_date_tercile"),
    )
)

annotated = (
    windowed
    .filter(F.col("days_since_prev_claim_date") >= 30)
    .select(
        "*",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.quarter(F.col("second_claim_date")).alias("second_claim_date_quarter"),
    )
)

sequenced = (
    annotated
    .select(
        "id",
        "status",
        "reported_at",
        "minutes_since_prev_reported_at",
        "claim_date_percent_rank",
        "claim_date_rank",
        "claim_date_rank_2",
        "days_since_prev_claim_date",
        "first_reported_at",
        "second_claim_date",
        "claim_date_tercile",
        "second_claim_date_quarter",
        F.round(F.avg("claim_date_percent_rank_2").over(w_rn), 2).alias("rn_avg_claim_date_percent_rank_2"),
        F.sum("second_claim_date_quarter").over(w_rn).alias("rn_sum_second_claim_date_quarter"),
        F.rank().over(w_all_by_minutes_since_prev_re).alias("minutes_since_prev_reported_at_rank"),
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "status",
        "reported_at",
        "minutes_since_prev_reported_at",
        "claim_date_percent_rank",
        "claim_date_rank",
        "claim_date_rank_2",
        "first_reported_at",
        "second_claim_date",
        "claim_date_tercile",
        "second_claim_date_quarter",
        "rn_sum_second_claim_date_quarter",
        "minutes_since_prev_reported_at_rank",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
    .orderBy(
        F.col("claim_date_percent_rank").desc(),
        F.col("second_claim_date").desc(),
    )
)
