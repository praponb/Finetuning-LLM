from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.date_trunc("month", "reported_at")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_reported_at_mont_2 = Window.partitionBy("adjuster_id", F.date_trunc("month", "reported_at")).orderBy("reported_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_reported_at_mont_3 = Window.partitionBy("adjuster_id", F.date_trunc("month", "reported_at")).orderBy("reported_at", "id")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_status_by_least_claimed_amou = Window.partitionBy("status").orderBy(F.col("least_claimed_amount_reported_at_percent_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_status_min_claimed_am = Window.orderBy("status_min_claimed_amount", "id")
w_status_by_least_claimed_amou_2 = Window.partitionBy("status").orderBy(F.col("least_claimed_amount_reported_at_percent_rank").desc(), "id")
w_status_by_least_claimed_amou_3 = Window.partitionBy("status").orderBy(F.col("least_claimed_amount_reported_at_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_least_claimed_amou_4 = Window.partitionBy("status").orderBy(F.col("least_claimed_amount_reported_at_percent_rank").desc(), "id").rowsBetween(-6, Window.currentRow)
w_reported_at_quartile = Window.partitionBy("reported_at_quartile")

filtered_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 11)) & (F.col("adjuster_id") <= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

sequenced = (
    filtered_claims
    .select(
        "*",
        F.min("claimed_amount").over(w_status).alias("status_min_claimed_amount"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.last("claim_date", ignorenulls=True).over(w_adjuster_id_reported_at_mont).alias("last_known_claim_date"),
        F.round(F.avg("claimed_amount").over(w_status), 2).alias("status_avg_claimed_amount"),
        F.max("claimed_amount").over(w_status).alias("status_max_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.max("approved_amount").over(w_adjuster_id_reported_at_mont_2).alias("approved_amount_max_7"),
        F.lag("reported_at").over(w_all_by_approved_amount).alias("prev_reported_at"),
        F.ntile(4).over(w_adjuster_id_reported_at_mont_3).alias("reported_at_quartile"),
        F.round(F.percent_rank().over(w_adjuster_id_reported_at_mont_3), 3).alias("reported_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
    )
)

enriched = (
    sequenced
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at_plus_1d",
        "status_min_claimed_amount",
        "rows_in_status",
        "last_known_claim_date",
        "status_avg_claimed_amount",
        "status_max_claimed_amount",
        "claimed_amount_vs_status_avg",
        "prev_reported_at",
        "reported_at_quartile",
        "reported_at_percent_rank",
        "approved_amount_cume_dist",
        F.concat(F.col("status"), F.lit("#"), F.col("reported_at_quartile").cast("string")).alias("status_ref"),
        F.least(F.col("claimed_amount"), F.col("reported_at_percent_rank")).alias("least_claimed_amount_reported_at_percent_rank"),
    )
)

sequenced_2 = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_min_claimed_amount",
        "rows_in_status",
        "last_known_claim_date",
        "status_avg_claimed_amount",
        "status_max_claimed_amount",
        "claimed_amount_vs_status_avg",
        "reported_at_percent_rank",
        "approved_amount_cume_dist",
        "status_ref",
        "least_claimed_amount_reported_at_percent_rank",
        F.round(F.avg("status_min_claimed_amount").over(w_status_by_least_claimed_amou), 2).alias("status_min_claimed_amount_avg_4"),
        F.lag("reported_at_quartile").over(w_all_by_status_min_claimed_am).alias("prev_reported_at_quartile"),
        (F.col("rows_in_status") - F.lag("rows_in_status").over(w_status_by_least_claimed_amou_2)).alias("rows_in_status_change"),
        F.first("claim_date", ignorenulls=True).over(w_status_by_least_claimed_amou_3).alias("first_known_claim_date"),
        F.round(F.avg("rows_in_status").over(w_status_by_least_claimed_amou_4), 2).alias("rows_in_status_avg_7"),
        F.ntile(5).over(w_all_by_status_min_claimed_am).alias("status_min_claimed_amount_quintile"),
        F.round(F.avg("status_max_claimed_amount").over(w_reported_at_quartile), 2).alias("reported_at_quartile_avg_status_max_claimed_amou"),
        F.round(F.cume_dist().over(w_status_by_least_claimed_amou_2), 3).alias("least_claimed_amount_reported_at_percent_rank_cu"),
    )
)

result = (
    sequenced_2
    .filter(F.col("status") != "IN_REVIEW")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "rows_in_status",
        "last_known_claim_date",
        "status_max_claimed_amount",
        "claimed_amount_vs_status_avg",
        "reported_at_percent_rank",
        "approved_amount_cume_dist",
        "status_ref",
        "least_claimed_amount_reported_at_percent_rank",
        "status_min_claimed_amount_avg_4",
        "prev_reported_at_quartile",
        "rows_in_status_change",
        "first_known_claim_date",
        "rows_in_status_avg_7",
        "status_min_claimed_amount_quintile",
        "reported_at_quartile_avg_status_max_claimed_amou",
        "least_claimed_amount_reported_at_percent_rank_cu",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
    .orderBy("claim_date_dayofweek", "id")
    .limit(5)
)
