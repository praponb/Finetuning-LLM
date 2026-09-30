from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_policy_id_claim_date_month_s = Window.partitionBy("policy_id", F.month("claim_date_month_start"))
w_claim_date_month_start_year = Window.partitionBy(F.year("claim_date_month_start")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_claims
    .filter((F.col("approved_amount") <= 14377.6) & (F.col("claim_date").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.rank().over(w_policy_id_by_approved_amount).alias("approved_amount_rank"),
        F.ntile(4).over(w_policy_id_by_approved_amount).alias("approved_amount_quartile"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_claim_date_month_s), 2).alias("approved_amount_vs_policy_id_claim_date_month_st"),
        F.last("approved_amount").over(w_claim_date_month_start_year).alias("last_approved_amount"),
        F.size(F.collect_set("claim_type").over(w_policy_id_claim_date_month_s)).alias("distinct_claim_type_in_policy_id_claim_date_mont"),
        F.round(F.avg("approved_amount").over(w_policy_id_claim_date_month_s), 2).alias("policy_id_claim_date_month_start_month_avg_appro"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        F.greatest(F.col("last_approved_amount"), F.col("approved_amount")).alias("greatest_last_approved_amount_approved_amount"),
        F.abs(F.col("approved_amount_rank") - 4).alias("approved_amount_rank_distance"),
    )
)

result = (
    annotated
    .rollup("approved_amount_rank_distance", "approved_amount_quartile", "policy_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("last_approved_amount"), 2).alias("avg_last_approved_amount"),
        F.sum(F.when(F.col("claim_type").isin("flood", "theft", "delay"), 1).otherwise(0)).alias("n_claim_type_in_flood_theft_de"),
    )
)
