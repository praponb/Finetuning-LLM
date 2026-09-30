from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_len = Window.partitionBy("status_len")
w_start_date_month_product_len = Window.partitionBy(F.date_trunc("month", "start_date"), "product_len").orderBy("min_claim_claimed_amount_filled")
w_start_date_month_product_len_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "product_len").orderBy("min_claim_claimed_amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claimed_amount").alias("min_claim_claimed_amount"))
)

with_stats = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_claimed_amount")
)

scored = (
    with_stats
    .filter(F.col("min_claim_claimed_amount") < 30040.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status_len",
        "min_claim_claimed_amount",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.coalesce(F.col("min_claim_claimed_amount"), F.lit(0.0)).alias("min_claim_claimed_amount_filled"),
    )
)

annotated = (
    scored
    .filter(F.col("premium") <= 4678.8)
    .select(
        "*",
        F.round(F.col("min_claim_claimed_amount") * 1.5, 2).alias("min_claim_claimed_amount_adj"),
    )
)

with_details = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on").between(F.lit("2024-05-23").cast("date"), F.lit("2025-05-01").cast("date")))
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "min_claim_claimed_amount",
        "start_date_plus_12m",
        "min_claim_claimed_amount_adj",
        "holder_name",
        "holder_region",
        F.count(F.lit(1)).over(w_status_len).alias("rows_in_status_len"),
        F.round(F.cume_dist().over(w_start_date_month_product_len), 3).alias("min_claim_claimed_amount_filled_cume_dist"),
        F.round(F.avg("product_len").over(w_status_len), 2).alias("status_len_avg_product_len"),
        F.sum("product_len").over(w_start_date_month_product_len_2).alias("running_product_len"),
    )
)

result = (
    ranked
    .select(
        "id",
        "product",
        "min_claim_claimed_amount",
        "start_date_plus_12m",
        "min_claim_claimed_amount_adj",
        "holder_region",
        "min_claim_claimed_amount_filled_cume_dist",
        "status_len_avg_product_len",
    )
    .orderBy(
        F.col("min_claim_claimed_amount").asc(),
        F.col("start_date_plus_12m").desc_nulls_first(),
    )
)
