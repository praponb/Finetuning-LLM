from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_by_reported = Window.partitionBy(F.month("claim_date")).orderBy(F.col("reported_at").desc(), "id")
w_reported_at_month_by_claim_d = Window.partitionBy(F.month("reported_at")).orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_claim_date_month_by_reported_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("reported_at").desc())

scoped = (
    df_claims
    .filter((F.col("approved_amount") < 17759.3) | (F.hour(F.col("reported_at")).between(8, 14)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.abs(F.col("claimed_amount") - 28426.9).alias("claimed_amount_distance"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

labeled = (
    scoped
    .select(
        "id",
        "claim_date",
        "reported_at",
        "claimed_amount_distance",
        F.round(F.col("claimed_amount_distance") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_distance_per_approved_amount"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "claim_date",
        "reported_at",
        F.dense_rank().over(w_claim_date_month_by_reported).alias("reported_at_dense_rank"),
        F.round(F.cume_dist().over(w_reported_at_month_by_claim_d), 3).alias("claim_date_cume_dist"),
        F.round(F.col("claimed_amount_distance") / F.nullif(F.sum("claimed_amount_distance").over(w_reported_at_to_date), F.lit(0)), 4).alias("claimed_amount_distance_share_of_reported_at_to_"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.dense_rank().over(w_claim_date_month_by_reported_2).alias("reported_at_dense_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "reported_at_dense_rank",
        "claim_date_cume_dist",
        "claimed_amount_distance_share_of_reported_at_to_",
        "rows_in_reported_at_to_date",
        "reported_at_dense_rank_2",
    )
    .orderBy(
        F.col("claim_date_cume_dist").asc(),
        F.col("claimed_amount_distance_share_of_reported_at_to_").desc_nulls_first(),
    )
)
