from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type", "status").orderBy(F.col("claim_date_plus_90d").desc(), F.col("id").desc())
w_adjuster_id = Window.partitionBy("adjuster_id")
w_status_by_claim_date_plus_90 = Window.partitionBy("status").orderBy("claim_date_plus_90d", "id").rowsBetween(-6, Window.currentRow)
w_status_by_claim_date_plus_90_2 = Window.partitionBy("status").orderBy("claim_date_plus_90d")
w_status_by_claim_date_plus_90_3 = Window.partitionBy("status").orderBy("claim_date_plus_90d", "id")

valid = (
    df_claims
    .filter((F.col("status") == "APPROVED") | (F.col("policy_id") >= 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

labeled = (
    valid
    .select(
        "*",
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

latest_per_key = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "claim_type_len",
        "claim_date_plus_90d",
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.max("claimed_amount").over(w_status_by_claim_date_plus_90).alias("claimed_amount_max_7"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id"),
        F.rank().over(w_status_by_claim_date_plus_90_2).alias("claim_date_plus_90d_rank"),
        F.lag("claimed_amount").over(w_status_by_claim_date_plus_90_3).alias("prev_claimed_amount"),
        F.round(F.percent_rank().over(w_status_by_claim_date_plus_90_3), 3).alias("claim_date_plus_90d_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "claim_type",
        "status",
        "claim_type_len",
        "claim_date_plus_90d",
        "rows_in_adjuster_id",
        "claim_date_plus_90d_rank",
        "prev_claimed_amount",
        "claim_date_plus_90d_percent_rank",
    )
    .orderBy(
        F.col("rows_in_adjuster_id").desc(),
        F.col("claim_date_plus_90d_percent_rank").desc_nulls_first(),
    )
)
