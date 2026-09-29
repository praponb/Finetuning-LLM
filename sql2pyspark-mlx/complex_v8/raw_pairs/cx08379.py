from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_all_by_start_date_prior_3 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-3, -1)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_all_by_end_date_running = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_policies
    .filter(~F.col("product").isin("TRAVEL", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.sum("premium").over(w_all_by_start_date_prior_3).alias("premium_sum_3_prior"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_start_date)).alias("days_since_prev_start_date"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "status_prefix",
        "start_date_percent_rank",
        "premium_sum_3_prior",
        "days_since_prev_start_date",
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.last("premium", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_premium"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

with_stats = (
    sequenced.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.max_claim_claimed_amount",
        "c.min_claim_approved_amount",
    )
)

holder_id_stats = (
    with_stats
    .groupBy("holder_id")
    .agg(F.percentile_approx("rows_in_holder_id_product", 0.5).alias("median_rows_in_holder_id_product"))
)

vs_group = (
    with_stats.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.rows_in_holder_id_product") >= F.col("g.median_rows_in_holder_id_product"))
    .select(
        "s.*",
        "g.median_rows_in_holder_id_product",
        F.round(F.col("s.rows_in_holder_id_product") - F.col("g.median_rows_in_holder_id_product"), 2).alias("rows_in_holder_id_product_minus_median"),
    )
)

with_children_claims = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.status_prefix",
        "s.start_date_percent_rank",
        "s.days_since_prev_start_date",
        "s.last_known_premium",
        "s.max_claim_claimed_amount",
        "s.median_rows_in_holder_id_product",
        "s.rows_in_holder_id_product_minus_median",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    with_children_claims
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date_percent_rank",
        "max_claim_claimed_amount",
        "median_rows_in_holder_id_product",
        "rows_in_holder_id_product_minus_median",
        "claim_adjuster_id",
        "claim_status",
    )
    .orderBy("rows_in_holder_id_product_minus_median")
)
