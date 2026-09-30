from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_approved_amount_dense_rank = Window.partitionBy("approved_amount_dense_rank")
w_status_by_approved_amount_de = Window.partitionBy("status").orderBy("approved_amount_dense_rank", "id")

covered = (
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
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

metrics = (
    covered
    .select(
        "*",
        F.first("premium", ignorenulls=True).over(w_policy_id_by_approved_amount).alias("first_known_premium"),
        F.max("claimed_amount").over(w_product).alias("product_max_claimed_amount"),
        F.dense_rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_dense_rank"),
        F.lag("claim_date").over(w_policy_id_by_approved_amount_2).alias("prev_claim_date"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.round(F.col("approved_amount_dense_rank") / F.nullif(F.sum("approved_amount_dense_rank").over(w_approved_amount_dense_rank), F.lit(0)), 4).alias("approved_amount_dense_rank_share_of_approved_amo"),
        (F.col("premium") - F.lag("premium").over(w_status_by_approved_amount_de)).alias("premium_change"),
        F.round(F.col("approved_amount_dense_rank") - F.avg("approved_amount_dense_rank").over(w_approved_amount_dense_rank), 2).alias("approved_amount_dense_rank_vs_approved_amount_de"),
    )
)

result = (
    sequenced
    .cube("status", "approved_amount_dense_rank")
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("product_max_claimed_amount"), 2).alias("stddev_product_max_claimed_amount"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("approved_amount_dense_rank").asc_nulls_last(),
    )
)
