from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc())
w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc())
w_claim_type = Window.partitionBy("claim_type")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_date_month_policy_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id")
w_policy_id_by_approved_amount_3 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc(), "id")
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy("reported_at")
w_claim_date_year_adjuster_id_2 = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_reported_at_t = Window.partitionBy("adjuster_id").orderBy("reported_at")
w_claim_type_by_reported_at_fu = Window.partitionBy("claim_type").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_id_by_approved_amount_4 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount_share_of_claim_type").desc())
w_reported_at_year_approved_am = Window.partitionBy(F.year("reported_at"), "approved_amount_rank").orderBy(F.col("running_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_year_approved_am_2 = Window.partitionBy(F.year("reported_at"), "approved_amount_rank").orderBy(F.col("running_count").asc_nulls_last(), "id")

scoped_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_type").isin("fire", "theft", "delay")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
    )
)

windowed = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        F.rank().over(w_policy_id_by_approved_amount).alias("approved_amount_rank"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claim_date_ti), 3).alias("claim_date_cume_dist"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_approved_amount_2), 2).alias("approved_amount_avg_3"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_month_policy_id), 2).alias("approved_amount_vs_claim_date_month_policy_id_av"),
        F.max("approved_amount").over(w_claim_type).alias("claim_type_max_approved_amount"),
        F.dense_rank().over(w_policy_id_by_approved_amount_3).alias("approved_amount_dense_rank"),
        F.round(F.percent_rank().over(w_claim_date_year_adjuster_id), 3).alias("reported_at_percent_rank"),
        F.count(F.lit(1)).over(w_claim_date_year_adjuster_id_2).alias("running_count"),
        F.round(F.percent_rank().over(w_adjuster_id_by_reported_at_t), 3).alias("reported_at_percent_rank_2"),
        F.last("claim_date").over(w_claim_type_by_reported_at_fu).alias("last_claim_date"),
        F.sum("approved_amount").over(w_claim_type).alias("claim_type_sum_approved_amount"),
    )
)

leaders = (
    windowed
    .filter(F.col("approved_amount_dense_rank") <= 5)
    .drop("approved_amount_dense_rank")
)

metrics = (
    leaders
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.round(F.percent_rank().over(w_policy_id_by_approved_amount_4), 3).alias("approved_amount_share_of_claim_type_percent_rank"),
        F.last("approved_amount", ignorenulls=True).over(w_reported_at_year_approved_am).alias("last_known_approved_amount"),
        F.row_number().over(w_reported_at_year_approved_am_2).alias("seq_no"),
        F.first("rows_in_claim_type", ignorenulls=True).over(w_reported_at_year_approved_am).alias("first_known_rows_in_claim_type"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "approved_amount_rank",
        "claim_date_cume_dist",
        "approved_amount_share_of_claim_type",
        "approved_amount_avg_3",
        "approved_amount_vs_claim_date_month_policy_id_av",
        "claim_type_max_approved_amount",
        "reported_at_percent_rank",
        "running_count",
        "reported_at_percent_rank_2",
        "last_claim_date",
        "claim_type_sum_approved_amount",
        "rows_in_adjuster_id_policy_id",
        "approved_amount_share_of_claim_type_percent_rank",
        "last_known_approved_amount",
        F.dayofweek(F.col("last_claim_date")).alias("last_claim_date_dayofweek"),
    )
    .orderBy(F.col("last_claim_date_dayofweek").desc_nulls_first(), "reported_at")
)
