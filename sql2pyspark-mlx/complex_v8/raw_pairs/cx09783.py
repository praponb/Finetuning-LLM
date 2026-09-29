from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_adjuster_id_by_premi = Window.partitionBy("product", "adjuster_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_adjuster_id_by_premi_2 = Window.partitionBy("product", "adjuster_id").orderBy("premium", "id").rowsBetween(-1, -1)
w_all_by_claim_date_moving_3 = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_product_adjuster_id_by_premi_3 = Window.partitionBy("product", "adjuster_id").orderBy("premium", "id")
w_claim_date_month_premium_ran = Window.partitionBy(F.date_trunc("month", "claim_date"), "premium_rank").orderBy("claimed_amount", "id").rowsBetween(-1, -1)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_product = Window.partitionBy("product")
w_claim_date_month_premium_ran_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "premium_rank").orderBy("claimed_amount")
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

metrics = (
    in_window
    .select(
        "*",
        F.first("premium").over(w_product_adjuster_id_by_premi).alias("first_premium"),
        F.sum("approved_amount").over(w_product_adjuster_id_by_premi_2).alias("approved_amount_sum_1_prior"),
        F.sum("premium").over(w_all_by_claim_date_moving_3).alias("premium_sum_4"),
        F.rank().over(w_product_adjuster_id_by_premi_3).alias("premium_rank"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.max("claimed_amount").over(w_claim_date_month_premium_ran).alias("claimed_amount_max_1_prior"),
        F.rank().over(w_all_by_approved_amount).alias("approved_amount_rank"),
        F.max("claimed_amount").over(w_product).alias("product_max_claimed_amount"),
        F.round(F.percent_rank().over(w_claim_date_month_premium_ran_2), 3).alias("claimed_amount_percent_rank"),
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.first("premium", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("first_known_premium"),
    )
)

result = (
    ranked
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "adjuster_id",
        "product",
        "premium",
        "premium_sum_4",
        "approved_amount_rank",
        "product_max_claimed_amount",
        "claimed_amount_percent_rank",
        "product_sum_premium",
        "first_known_premium",
        F.date_add(F.col("claim_date"), 7).alias("claim_date_plus_7d"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("claimed_amount_per_premium"),
    )
    .orderBy(F.col("claimed_amount").desc(), "product")
)
