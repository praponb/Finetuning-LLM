from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_status_policy_id_by_approved = Window.partitionBy("status", "policy_id").orderBy(F.col("approved_amount").desc(), "id")
w_status_policy_id_by_approved_2 = Window.partitionBy("status", "policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_claim_date_month = Window.partitionBy("product", F.month("claim_date"))

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
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

latest_per_key = (
    in_window
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest_per_key
    .select(
        "*",
        F.row_number().over(w_status_policy_id_by_approved).alias("seq_no"),
        F.max("claimed_amount").over(w_status_policy_id_by_approved_2).alias("max_claimed_amount_so_far"),
        F.sum("premium").over(w_product_claim_date_month).alias("product_claim_date_month_sum_premium"),
    )
)

annotated = (
    ranked
    .filter(F.col("status") == "PAID")
    .select(
        "*",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "premium",
        "max_claimed_amount_so_far",
        "product_claim_date_month_sum_premium",
        "product_prefix",
        "status_len",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
    .orderBy("status_len", "premium")
)
