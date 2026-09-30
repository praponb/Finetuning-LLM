from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_product_claim_date_month = Window.partitionBy("product", F.date_trunc("month", "claim_date"))
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

ranked = (
    covered
    .select(
        "id",
        "claimed_amount",
        "claim_type",
        "approved_amount",
        "product",
        "premium",
        F.last("premium", ignorenulls=True).over(w_policy_id_by_claimed_amount).alias("last_known_premium"),
        F.row_number().over(w_policy_id_by_claimed_amount_2).alias("seq_no"),
        F.count(F.lit(1)).over(w_product_claim_date_month).alias("rows_in_product_claim_date_month"),
        F.lead("claim_date").over(w_policy_id_by_claimed_amount_2).alias("next_claim_date"),
        F.first("approved_amount").over(w_policy_id_by_claimed_amount_3).alias("first_approved_amount"),
        F.max("premium").over(w_product_claim_date_month).alias("product_claim_date_month_max_premium"),
    )
)

result = (
    ranked
    .groupingSets(
        [["product", "seq_no"], ["product"], ["seq_no"], []],
        "product", "seq_no",
    )
    .agg(
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.sum(F.when(F.col("product").isNotNull(), F.col("seq_no")).otherwise(0)).alias("seq_no_product_is_not_null"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "seq_no",
        "distinct_claim_types",
        "seq_no_product_is_not_null",
    )
)
