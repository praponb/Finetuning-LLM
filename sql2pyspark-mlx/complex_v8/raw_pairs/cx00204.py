from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claim_type_len").asc_nulls_last(), "id")
w_policy_id = Window.partitionBy("policy_id")

recent = df_claims

tagged = (
    recent
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        (F.col("claim_type_len") - F.lag("claim_type_len").over(w_adjuster_id_policy_id_by_cla)).alias("claim_type_len_change"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "claim_type_prefix",
        "status_clean",
        "claim_type_len_change",
    )
    .orderBy(F.col("adjuster_id").desc())
)
