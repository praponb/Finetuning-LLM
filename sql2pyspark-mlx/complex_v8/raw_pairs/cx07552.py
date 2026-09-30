from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_product_by_premium = Window.partitionBy("policy_id", "product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_product_by_premium_2 = Window.partitionBy("policy_id", "product").orderBy("premium", "id")
w_second_claim_date_month = Window.partitionBy(F.month("second_claim_date"))
w_product_first_claim_date_mon = Window.partitionBy("product", F.month("first_claim_date")).orderBy(F.col("claimed_amount").desc(), "id")

covered = (
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
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

windowed = (
    covered
    .select(
        "*",
        F.nth_value("claim_date", 2).over(w_policy_id_product_by_premium).alias("second_claim_date"),
        F.first("claim_date").over(w_policy_id_product_by_premium).alias("first_claim_date"),
        F.lead("claimed_amount", 3).over(w_policy_id_product_by_premium_2).alias("next3_claimed_amount"),
        F.row_number().over(w_policy_id_product_by_premium_2).alias("seq_no"),
    )
)

annotated = (
    windowed
    .filter(F.col("first_claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount",
        "product",
        "premium",
        "second_claim_date",
        "first_claim_date",
        "next3_claimed_amount",
        "seq_no",
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.date_trunc("week", F.col("second_claim_date")).alias("second_claim_date_week"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.size(F.collect_set("product").over(w_second_claim_date_month)).alias("distinct_product_in_second_claim_date_month"),
        F.ntile(3).over(w_product_first_claim_date_mon).alias("claimed_amount_tercile"),
        F.size(F.collect_set("status_or_unknown").over(w_second_claim_date_month)).alias("distinct_status_or_unknown_in_second_claim_date_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "product",
        "premium",
        "second_claim_date",
        "first_claim_date",
        "seq_no",
        "second_claim_date_week",
        "status_or_unknown",
        "distinct_product_in_second_claim_date_month",
        "claimed_amount_tercile",
    )
)
