from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product = Window.partitionBy("policy_product")
w_all_by_days_since_claim_date = Window.orderBy(F.col("days_since_claim_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_adjuster_id_by_policy_premiu = Window.partitionBy("adjuster_id").orderBy("policy_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_claim_date_2 = Window.orderBy(F.col("days_since_claim_date").desc(), "id")
w_all_by_days_since_claim_date_3 = Window.orderBy("days_since_claim_date_quartile", "id")
w_adjuster_id_policy_holder_id = Window.partitionBy("adjuster_id", "policy_holder_id")
w_last_reported_at_year_policy = Window.partitionBy(F.year("last_reported_at"), "policy_holder_id").orderBy("last_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_reported_at_year_policy_2 = Window.partitionBy(F.year("last_reported_at"), "policy_holder_id").orderBy("last_reported_at", "id")
w_next_reported_at_month_by_ro = Window.partitionBy(F.month("next_reported_at")).orderBy(F.col("rows_in_policy_product").asc_nulls_last())
w_all_by_next_reported_at_movi = Window.orderBy(F.col("next_reported_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_next_reported_at_month_by_ro_2 = Window.partitionBy(F.month("next_reported_at")).orderBy(F.col("rows_in_policy_product").asc_nulls_last(), "id")

prepared_claims = (
    df_claims
    .filter(F.col("approved_amount") >= 4234.9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

joined = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.days_since_claim_date",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

ranked = (
    joined
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "days_since_claim_date",
        "policy_holder_id",
        "policy_product",
        F.count(F.lit(1)).over(w_policy_product).alias("rows_in_policy_product"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_policy_product), F.lit(0)), 4).alias("policy_premium_share_of_policy_product"),
        F.sum("policy_premium").over(w_all_by_days_since_claim_date).alias("policy_premium_sum_2"),
        F.last("reported_at").over(w_adjuster_id_by_policy_premiu).alias("last_reported_at"),
        F.lead("reported_at").over(w_all_by_days_since_claim_date_2).alias("next_reported_at"),
        F.max("days_since_claim_date").over(w_policy_product).alias("policy_product_max_days_since_claim_date"),
        F.ntile(4).over(w_all_by_days_since_claim_date_2).alias("days_since_claim_date_quartile"),
        F.size(F.collect_set("policy_id").over(w_policy_product)).alias("distinct_policy_id_in_policy_product"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "days_since_claim_date",
        "policy_holder_id",
        "rows_in_policy_product",
        "policy_premium_share_of_policy_product",
        "policy_premium_sum_2",
        "last_reported_at",
        "next_reported_at",
        "policy_product_max_days_since_claim_date",
        "days_since_claim_date_quartile",
        F.concat(F.col("policy_product"), F.lit("#"), F.col("policy_id").cast("string")).alias("policy_product_ref"),
        F.substring(F.trim(F.col("policy_product")), 1, 2).alias("policy_product_prefix"),
        (F.col("policy_premium_sum_2") - F.col("days_since_claim_date")).alias("policy_premium_sum_2_minus_days_since_claim_date"),
    )
)

ranked_2 = (
    scored
    .select(
        "*",
        F.ntile(5).over(w_all_by_days_since_claim_date_3).alias("days_since_claim_date_quartile_quintile"),
        F.min("rows_in_policy_product").over(w_adjuster_id_policy_holder_id).alias("adjuster_id_policy_holder_id_min_rows_in_policy_"),
        F.nth_value("reported_at", 2).over(w_last_reported_at_year_policy).alias("second_reported_at"),
        F.lag("days_since_claim_date").over(w_last_reported_at_year_policy_2).alias("prev_days_since_claim_date"),
        F.rank().over(w_next_reported_at_month_by_ro).alias("rows_in_policy_product_rank"),
        F.round(F.avg("rows_in_policy_product").over(w_all_by_next_reported_at_movi), 2).alias("rows_in_policy_product_avg_7"),
        F.lag("days_since_claim_date", 1, 0).over(w_next_reported_at_month_by_ro_2).alias("prev_days_since_claim_date_2"),
    )
)

summary = (
    ranked_2
    .groupBy("days_since_claim_date_quartile", "rows_in_policy_product_rank")
    .agg(
        F.round(F.avg("rows_in_policy_product_avg_7"), 2).alias("avg_rows_in_policy_product_avg_7"),
        F.min("prev_days_since_claim_date").alias("min_prev_days_since_claim_date"),
    )
)

result = (
    summary
    .select(
        "days_since_claim_date_quartile",
        "rows_in_policy_product_rank",
        "min_prev_days_since_claim_date",
    )
    .orderBy(
        F.col("days_since_claim_date_quartile").desc(),
        F.col("rows_in_policy_product_rank").desc(),
    )
)
