from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_policy_status = Window.partitionBy("policy_id", "policy_status")
w_policy_id_adjuster_id_by_pre = Window.partitionBy("policy_id", "adjuster_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_id_by_pre_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("premium", "id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_details = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.premium",
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.size(F.collect_set("adjuster_id").over(w_policy_id_policy_status)).alias("distinct_adjuster_id_in_policy_id_policy_status"),
        F.last("claimed_amount", ignorenulls=True).over(w_policy_id_adjuster_id_by_pre).alias("last_known_claimed_amount"),
        F.size(F.collect_set("policy_holder_id").over(w_policy_id_policy_status)).alias("distinct_policy_holder_id_in_policy_id_policy_st"),
        F.ntile(3).over(w_policy_id_adjuster_id_by_pre_2).alias("premium_tercile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "adjuster_id",
        "premium",
        "policy_status",
        "policy_premium",
        "policy_holder_id",
        "distinct_adjuster_id_in_policy_id_policy_status",
        "last_known_claimed_amount",
        F.substring(F.trim(F.col("policy_status")), 1, 3).alias("policy_status_prefix"),
        F.concat(F.col("policy_status"), F.lit("#"), F.col("premium_tercile").cast("string")).alias("policy_status_ref"),
    )
    .orderBy(F.col("adjuster_id").desc(), "id")
    .limit(5)
)
