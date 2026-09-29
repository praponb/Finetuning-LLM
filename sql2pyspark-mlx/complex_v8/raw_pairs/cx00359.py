from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")

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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

sequenced = (
    in_window
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "product",
        F.size(F.collect_set("claim_type").over(w_product)).alias("distinct_claim_type_in_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "claim_type",
        "product",
        "distinct_claim_type_in_product",
        F.coalesce(F.col("premium_share_of_product"), F.lit(0.0)).alias("premium_share_of_product_filled"),
        F.abs(F.col("approved_amount") - 14595.6).alias("approved_amount_distance"),
    )
    .orderBy(
        F.col("distinct_claim_type_in_product").desc_nulls_first(),
        F.col("product").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
