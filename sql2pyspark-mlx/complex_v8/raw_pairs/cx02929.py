from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_year_adjuster_id = Window.partitionBy(F.year("reported_at"), "adjuster_id")
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy("reported_at", "id").rowsBetween(-6, Window.currentRow)
w_reported_at_month_adjuster_i = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_reported_at_year = Window.partitionBy("adjuster_id", F.year("reported_at"))
w_adjuster_id_reported_at_mont_2 = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy("reported_at", "id")
w_reported_at_month_adjuster_i_2 = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id").orderBy("approved_amount", "id")
w_reported_at_month_adjuster_i_3 = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id").orderBy("approved_amount")
w_reported_at_month_adjuster_i_4 = Window.partitionBy(F.month("reported_at"), "adjuster_id").orderBy("approved_amount")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())

cleaned_claims = (
    df_claims
    .filter(F.col("approved_amount") > 2569.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

sequenced = (
    cleaned_claims
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "reported_at_label",
        F.round(F.avg("claimed_amount").over(w_reported_at_year_adjuster_id), 2).alias("reported_at_year_adjuster_id_avg_claimed_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_year_adjuster_id), 2).alias("approved_amount_vs_reported_at_year_adjuster_id_"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_reported_at_mont), 2).alias("claimed_amount_avg_7"),
        F.first("approved_amount").over(w_reported_at_month_adjuster_i).alias("first_approved_amount"),
        F.count(F.lit(1)).over(w_adjuster_id_reported_at_year).alias("rows_in_adjuster_id_reported_at_year"),
        F.sum("claimed_amount").over(w_adjuster_id_reported_at_year).alias("adjuster_id_reported_at_year_sum_claimed_amount"),
        F.row_number().over(w_adjuster_id_reported_at_mont_2).alias("seq_no"),
        F.lead("approved_amount", 1, 0).over(w_reported_at_month_adjuster_i_2).alias("next_approved_amount"),
        F.rank().over(w_reported_at_month_adjuster_i_3).alias("approved_amount_rank"),
        F.rank().over(w_reported_at_month_adjuster_i_4).alias("approved_amount_rank_2"),
        F.min("claimed_amount").over(w_adjuster_id_reported_at_year).alias("adjuster_id_reported_at_year_min_claimed_amount"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_adjuster_id_by_approved_amou))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.percent_rank().over(w_policy_id_by_reported_at_tie), 3).alias("reported_at_percent_rank"),
        F.rank().over(w_adjuster_id_reported_at_mont_2).alias("reported_at_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("first_approved_amount") <= 24718.5)
    .select(
        "id",
        "reported_at",
        "reported_at_label",
        "approved_amount_vs_reported_at_year_adjuster_id_",
        "claimed_amount_avg_7",
        "first_approved_amount",
        "rows_in_adjuster_id_reported_at_year",
        "adjuster_id_reported_at_year_sum_claimed_amount",
        "seq_no",
        "next_approved_amount",
        "approved_amount_rank",
        "approved_amount_rank_2",
        "adjuster_id_reported_at_year_min_claimed_amount",
        "minutes_since_prev_reported_at",
        "reported_at_percent_rank",
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
    .orderBy("reported_at_label", "id")
    .limit(5)
)
