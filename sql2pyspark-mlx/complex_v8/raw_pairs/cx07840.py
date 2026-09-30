from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_product_by = Window.partitionBy(F.month("claim_date"), "product").orderBy(F.col("avg_claim_approved_amount").desc())
w_product_status = Window.partitionBy("product", "status")
w_status_by_claimed_amount_ful = Window.partitionBy("status").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_product_by_2 = Window.partitionBy(F.month("claim_date"), "product").orderBy(F.col("avg_claim_approved_amount").desc(), "id").rowsBetween(-2, -1)
w_last_claim_reported_at_to_da = Window.partitionBy(F.to_date("last_claim_reported_at")).orderBy("avg_claim_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_claim_reported_at_to_da_2 = Window.partitionBy(F.to_date("last_claim_reported_at")).orderBy("avg_claim_approved_amount", "id")

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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

profiled = (
    in_window.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        "c.avg_claim_approved_amount",
        "c.last_claim_reported_at",
    )
)

sequenced = (
    profiled
    .select(
        "*",
        F.dense_rank().over(w_claim_date_month_product_by).alias("avg_claim_approved_amount_dense_rank"),
        F.max("avg_claim_approved_amount").over(w_product_status).alias("product_status_max_avg_claim_approved_amount"),
        F.nth_value("min_claim_claimed_amount", 2).over(w_status_by_claimed_amount_ful).alias("second_min_claim_claimed_amount"),
        F.max("min_claim_claimed_amount").over(w_claim_date_month_product_by_2).alias("min_claim_claimed_amount_max_2_prior"),
        F.last("last_claim_reported_at", ignorenulls=True).over(w_last_claim_reported_at_to_da).alias("last_known_last_claim_reported_at"),
        F.lag("claim_date", 3).over(w_last_claim_reported_at_to_da_2).alias("prev3_claim_date"),
    )
)

combined_details_adjuster = (
    sequenced.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.status",
        "s.premium",
        "s.min_claim_claimed_amount",
        "s.avg_claim_approved_amount",
        "s.product_status_max_avg_claim_approved_amount",
        "s.second_min_claim_claimed_amount",
        "s.min_claim_claimed_amount_max_2_prior",
        "s.last_known_last_claim_reported_at",
        "s.prev3_claim_date",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    combined_details_adjuster
    .select("min_claim_claimed_amount")
    .distinct()
    .orderBy(F.col("min_claim_claimed_amount").asc())
)
