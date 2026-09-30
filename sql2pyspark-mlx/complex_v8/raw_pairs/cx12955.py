from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_policy_product_by = Window.partitionBy("claim_type", "policy_product").orderBy(F.col("claim_date").desc(), "id")
w_claim_type_policy_product_by_2 = Window.partitionBy("claim_type", "policy_product").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_holder_id_adjuster_id = Window.partitionBy("policy_holder_id", "adjuster_id").orderBy("reported_at")
w_policy_holder_id_policy_id = Window.partitionBy("policy_holder_id", "policy_id")
w_claim_type_policy_product_by_3 = Window.partitionBy("claim_type", "policy_product").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-2, -1)
w_policy_holder_id_adjuster_id_2 = Window.partitionBy("policy_holder_id", "adjuster_id").orderBy("reported_at", "id").rowsBetween(-1, Window.currentRow)
w_claim_type_policy_product_by_4 = Window.partitionBy("claim_type", "policy_product").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_product_policy_id_by = Window.partitionBy("policy_product", "policy_id").orderBy("reported_at")

valid_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

joined_policy = (
    valid_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_date_dayofweek",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

with_trends = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "policy_product",
        F.max("approved_amount").over(w_adjuster_id).alias("adjuster_id_max_approved_amount"),
        F.round(F.cume_dist().over(w_claim_type_policy_product_by), 3).alias("claim_date_cume_dist"),
        F.sum("approved_amount").over(w_claim_type_policy_product_by_2).alias("running_approved_amount"),
        F.rank().over(w_policy_holder_id_adjuster_id).alias("reported_at_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_holder_id_policy_id), 2).alias("approved_amount_vs_policy_holder_id_policy_id_av"),
        F.round(F.avg("claim_date_dayofweek").over(w_adjuster_id), 2).alias("adjuster_id_avg_claim_date_dayofweek"),
        F.sum("approved_amount").over(w_claim_type_policy_product_by_3).alias("approved_amount_sum_2_prior"),
        F.round(F.avg("approved_amount").over(w_policy_holder_id_adjuster_id_2), 2).alias("approved_amount_avg_2"),
        F.first("claim_date").over(w_claim_type_policy_product_by_4).alias("first_claim_date"),
        F.round(F.cume_dist().over(w_policy_product_policy_id_by), 3).alias("reported_at_cume_dist"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("reported_at_rank") == 1) & (F.col("running_approved_amount") < 425000.0))
)

result = (
    first_rows
    .filter(F.col("adjuster_id_max_approved_amount") > 100000.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "adjuster_id_max_approved_amount",
        "approved_amount_vs_policy_holder_id_policy_id_av",
        "adjuster_id_avg_claim_date_dayofweek",
        "approved_amount_sum_2_prior",
        "reported_at_cume_dist",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
        F.concat(F.col("policy_product"), F.lit("#"), F.col("id").cast("string")).alias("policy_product_ref"),
    )
    .orderBy(
        "adjuster_id_max_approved_amount",
        F.col("adjuster_id_avg_claim_date_dayofweek").asc(),
    )
)
