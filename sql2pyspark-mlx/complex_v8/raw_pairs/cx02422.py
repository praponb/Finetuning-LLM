from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_product = Window.partitionBy(F.year("claim_date"), "product")
w_policy_id_status_by_adjuster = Window.partitionBy("policy_id", "status").orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

enriched = (
    in_window.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.status",
        "s.product",
        "s.premium",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_claim_date_year_product), F.lit(0)), 4).alias("premium_share_of_claim_date_year_product"),
        F.min("adjuster_level").over(w_claim_date_year_product).alias("claim_date_year_product_min_adjuster_level"),
        F.sum("claimed_amount").over(w_policy_id_status_by_adjuster).alias("running_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_date_year_product).alias("rows_in_claim_date_year_product"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["status", "product"], ["status"], ["product"], []],
        "status", "product",
    )
    .agg(
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("product").asc_nulls_last())
)
