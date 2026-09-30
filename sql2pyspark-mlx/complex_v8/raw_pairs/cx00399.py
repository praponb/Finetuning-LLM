from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_policy_id_claim_date_year_ye = Window.partitionBy("policy_id", F.year("claim_date_year")).orderBy("reported_at", "id").rowsBetween(-1, -1)
w_policy_id_claim_date_year_ye_2 = Window.partitionBy("policy_id", F.year("claim_date_year")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("claim_type").orderBy(F.col("approved_amount_share_of_claim_type").desc(), F.col("id").desc())

valid = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "claimed_amount_filled",
        F.round(F.avg("claimed_amount_filled").over(w_claim_type), 2).alias("claim_type_avg_claimed_amount_filled"),
        F.max("claimed_amount").over(w_policy_id_claim_date_year_ye).alias("claimed_amount_max_1_prior"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.min("approved_amount").over(w_policy_id_claim_date_year_ye_2).alias("min_approved_amount_so_far"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount_filled",
        "claim_type_avg_claimed_amount_filled",
        "rows_in_claim_type",
        "approved_amount_share_of_claim_type",
    )
    .orderBy("rows_in_claim_type")
)
