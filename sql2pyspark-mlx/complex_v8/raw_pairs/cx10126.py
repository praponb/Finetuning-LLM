from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_status_by_c = Window.partitionBy(F.month("claim_date"), "status").orderBy("claim_date")
w_claim_date_month_status_by_c_2 = Window.partitionBy(F.month("claim_date"), "status").orderBy("claim_date", "id")
w_claim_date_month_status_by_c_3 = Window.partitionBy(F.month("claim_date"), "status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

sequenced = (
    matched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "premium",
        F.round(F.cume_dist().over(w_claim_date_month_status_by_c), 3).alias("claim_date_cume_dist"),
        F.dense_rank().over(w_claim_date_month_status_by_c_2).alias("claim_date_dense_rank"),
        F.nth_value("approved_amount", 2).over(w_claim_date_month_status_by_c_3).alias("second_approved_amount"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.status",
        "s.claim_date_cume_dist",
        "s.second_approved_amount",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        "po.holder_name",
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

result = (
    combined_details
    .filter(F.upper(F.trim(F.col("holder_name"))).like("D%"))
    .select(
        "id",
        "policy_id",
        "status",
        "policy_product",
        "holder_name",
        "holder_joined_on",
        F.date_format(F.col("policy_start_date"), "yyyy-MM-dd").alias("policy_start_date_label"),
    )
    .orderBy(F.col("status").asc())
)
