from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_by_claim_da = Window.partitionBy(F.month("claim_date")).orderBy("claim_date", "id").rowsBetween(-3, Window.currentRow)
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_claim_date_month_by_claim_da_2 = Window.partitionBy(F.month("claim_date")).orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_claim_da_3 = Window.partitionBy(F.month("claim_date")).orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_by_claim_da_4 = Window.partitionBy(F.month("claim_date")).orderBy("claim_date", "id")
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")

scoped_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 44991.5) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

metrics = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        F.round(F.avg("approved_amount").over(w_claim_date_month_by_claim_da), 2).alias("approved_amount_avg_4"),
        F.last("claimed_amount", ignorenulls=True).over(w_adjuster_id_by_approved_amou).alias("last_known_claimed_amount"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_percent_rank"),
        F.dense_rank().over(w_all_by_claim_date).alias("claim_date_dense_rank"),
        F.max("approved_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_max_approved_amount"),
        F.sum("claimed_amount").over(w_claim_date_month_by_claim_da_2).alias("running_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("claimed_amount_vs_adjuster_id_policy_id_avg"),
        F.nth_value("claim_date", 2).over(w_claim_date_month_by_claim_da_3).alias("second_claim_date"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_claim_date)).alias("approved_amount_change"),
        F.ntile(5).over(w_all_by_claim_date).alias("claim_date_quintile"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_id"),
        F.lag("claimed_amount", 3).over(w_claim_date_month_by_claim_da_4).alias("prev3_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_policy_id"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
        (F.floor(F.col("prev3_claimed_amount") / 10000) * 10000).alias("prev3_claimed_amount_bucket"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

stats = (
    labeled
    .groupBy("claim_date_dayofweek", "policy_id", "claim_date_quintile")
    .agg(
        F.max("claim_date_year").alias("last_claim_date_year"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.min("claim_date_year").alias("first_claim_date_year"),
        F.round(F.stddev("last_known_claimed_amount"), 2).alias("stddev_last_known_claimed_amount"),
        F.round(F.stddev("approved_amount_change"), 2).alias("stddev_approved_amount_change"),
        F.count("*").alias("row_count"),
    )
)

result = (
    stats
    .filter(F.col("stddev_last_known_claimed_amount") < 42500.0)
    .select(
        "claim_date_dayofweek",
        "policy_id",
        "last_claim_date_year",
        "stddev_last_known_claimed_amount",
        "stddev_approved_amount_change",
        "row_count",
    )
    .orderBy("last_claim_date_year")
)
