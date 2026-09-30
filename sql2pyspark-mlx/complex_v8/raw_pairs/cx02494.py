from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())
w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("claim_date", "id")

prepared_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

with_activity = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id") >= 5) & (F.col("c.approved_amount").between(492.6, 16404.6)), "left_anti")
)

latest_per_key = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    latest_per_key
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_claim_type).alias("rows_in_adjuster_id_claim_type"),
        F.last("claim_date", ignorenulls=True).over(w_policy_id_adjuster_id_by_cla).alias("last_known_claim_date"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_adjuster_id_by_cla_2)).alias("claimed_amount_change"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_claim_type"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_claim_type"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.lower(F.regexp_replace(F.trim(F.col("status_claim_type_label")), " ", "_")).alias("status_claim_type_label_slug"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status_claim_type_label",
        "last_known_claim_date",
        "claimed_amount_change",
        "approved_amount_share_of_adjuster_id_claim_type",
        "claimed_amount_share_of_adjuster_id_claim_type",
        "claim_type_prefix",
        F.abs(F.col("approved_amount") - 12305.3).alias("approved_amount_distance"),
    )
    .orderBy("last_known_claim_date")
)
