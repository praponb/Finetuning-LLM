from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

sequenced = (
    in_window
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "premium",
        F.lag("claimed_amount").over(w_all_by_claim_date).alias("prev_claimed_amount"),
        F.max("claimed_amount").over(w_adjuster_id).alias("adjuster_id_max_claimed_amount"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "claimed_amount",
        "product",
        "adjuster_id_max_claimed_amount",
        (F.floor(F.col("prev_claimed_amount") / 10000) * 10000).alias("prev_claimed_amount_bucket"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
    .orderBy("id")
)
