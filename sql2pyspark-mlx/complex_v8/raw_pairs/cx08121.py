from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_claim_date_moving = Window.partitionBy("product").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_product_by_claim_date = Window.partitionBy("product").orderBy(F.col("claim_date").desc(), "id")
w_claim_date_year_claim_type = Window.partitionBy(F.year("claim_date"), "claim_type")

matched = (
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

with_activity = (
    matched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date").isNotNull()) & (F.hour(F.col("c.reported_at")).between(10, 16)), "left_anti")
    .filter(F.col("premium") > 1132.1)
)

windowed = (
    with_activity
    .select(
        "*",
        F.max("premium").over(w_product_by_claim_date_moving).alias("premium_max_2"),
        F.rank().over(w_product_by_claim_date).alias("claim_date_rank"),
        F.round(F.avg("claimed_amount").over(w_claim_date_year_claim_type), 2).alias("claim_date_year_claim_type_avg_claimed_amount"),
        F.lead("claim_date").over(w_product_by_claim_date).alias("next_claim_date"),
    )
)

result = (
    windowed
    .groupingSets(
        [["policy_id", "product", "claim_type"], ["policy_id"], ["product"], []],
        "policy_id", "product", "claim_type",
    )
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.sum("claim_date_rank").alias("total_claim_date_rank"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .select(
        "policy_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "is_total_policy_id",
        "total_claim_date_rank",
        "total_claimed_amount",
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
