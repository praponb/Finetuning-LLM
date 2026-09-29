from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

windowed = (
    in_window
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_product), F.lit(0)), 4).alias("claimed_amount_share_of_product"),
        F.max("approved_amount").over(w_adjuster_id_by_approved_amou).alias("max_approved_amount_so_far"),
        F.sum("approved_amount").over(w_product).alias("product_sum_approved_amount"),
    )
)

with_details_adjuster = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    with_details_adjuster
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "claimed_amount_share_of_product",
        "max_approved_amount_so_far",
        "product_sum_approved_amount",
        "adjuster_level",
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
    .orderBy("id", F.col("product_sum_approved_amount").desc())
)
