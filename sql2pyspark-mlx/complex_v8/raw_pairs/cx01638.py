from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_claim_date_month_by_cla = Window.partitionBy(F.month("last_claim_date")).orderBy("claim_date_span_days", "claim_date_ym", "product", "adjuster_id")
w_distinct_products = Window.partitionBy("distinct_products")

joined_ranges = (
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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

grouped = (
    joined_ranges
    .groupBy(
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "product",
        "adjuster_id",
    )
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        (F.col("claim_date_span_days") - F.lag("claim_date_span_days").over(w_last_claim_date_month_by_cla)).alias("claim_date_span_days_change"),
        F.sum("avg_claimed_amount").over(w_distinct_products).alias("distinct_products_sum_avg_claimed_amount"),
    )
)

result = (
    ranked
    .filter(F.col("claim_date_span_days") >= 140)
    .select(
        "claim_date_ym",
        "product",
        "adjuster_id",
        "last_claim_date",
        "claim_date_span_days",
        "min_approved_amount",
        "distinct_products",
        "claim_date_span_days_change",
        F.lower(F.regexp_replace(F.trim(F.col("claim_date_ym")), " ", "_")).alias("claim_date_ym_slug"),
    )
    .orderBy(F.col("claim_date_ym_slug").desc())
)
