from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc(), "id")
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy("claimed_amount")
w_product_claim_type_by_claime = Window.partitionBy("product", "claim_type").orderBy("claimed_amount", "id").rowsBetween(-1, Window.currentRow)
w_claim_date_year = Window.partitionBy(F.year("claim_date"))
w_product_claim_type_by_claime_2 = Window.partitionBy("product", "claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_claim_type_by_claime_3 = Window.partitionBy("product", "claim_type").orderBy("claimed_amount")

matched = (
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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

ranked = (
    matched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "product",
        "premium",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.lead("claimed_amount").over(w_claim_type_by_claimed_amount).alias("next_claimed_amount"),
        F.min("claimed_amount").over(w_product).alias("product_min_claimed_amount"),
        F.max("claimed_amount").over(w_product).alias("product_max_claimed_amount"),
        F.round(F.cume_dist().over(w_claim_type_by_claimed_amount_2), 3).alias("claimed_amount_cume_dist"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "product",
        "premium",
        "premium_share_of_product",
        "product_min_claimed_amount",
        "product_max_claimed_amount",
        "rows_in_product",
        F.max("premium_share_of_product").over(w_product_claim_type_by_claime).alias("premium_share_of_product_max_2"),
        F.size(F.collect_set("claim_type").over(w_claim_date_year)).alias("distinct_claim_type_in_claim_date_year"),
        F.round(F.avg("product_min_claimed_amount").over(w_claim_date_year), 2).alias("claim_date_year_avg_product_min_claimed_amount"),
        F.size(F.collect_set("product").over(w_claim_date_year)).alias("distinct_product_in_claim_date_year"),
        F.last("claim_date", ignorenulls=True).over(w_product_claim_type_by_claime_2).alias("last_known_claim_date"),
        F.round(F.cume_dist().over(w_product_claim_type_by_claime_3), 3).alias("claimed_amount_cume_dist_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "product",
        "premium",
        "product_min_claimed_amount",
        "product_max_claimed_amount",
        "rows_in_product",
        "premium_share_of_product_max_2",
        "claim_date_year_avg_product_min_claimed_amount",
        "distinct_product_in_claim_date_year",
        "last_known_claim_date",
    )
    .orderBy(
        F.col("distinct_product_in_claim_date_year").asc(),
        F.col("premium").asc(),
        "id",
    )
    .limit(20)
)
