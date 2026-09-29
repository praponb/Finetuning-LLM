from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_product_by = Window.partitionBy(F.month("claim_date"), "product").orderBy("days_since_claim_date", "id")
w_product = Window.partitionBy("product")
w_claim_date_month_product_by_2 = Window.partitionBy(F.month("claim_date"), "product").orderBy("days_since_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

in_window = (
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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

tagged = (
    in_window
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        F.concat(F.col("product"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("product_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

with_windows = (
    tagged
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "product",
        "product_ref",
        "days_since_claim_date",
        (F.col("days_since_claim_date") - F.lag("days_since_claim_date").over(w_claim_date_month_product_by)).alias("days_since_claim_date_change"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.sum("days_since_claim_date").over(w_claim_date_month_product_by_2).alias("running_days_since_claim_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claimed_amount",
        "days_since_claim_date_change",
        "rows_in_product",
        "running_days_since_claim_date",
        F.coalesce(F.col("days_since_claim_date"), F.lit(0)).alias("days_since_claim_date_filled"),
    )
    .orderBy(
        F.col("rows_in_product").desc_nulls_first(),
        F.col("claimed_amount").desc_nulls_first(),
    )
)
