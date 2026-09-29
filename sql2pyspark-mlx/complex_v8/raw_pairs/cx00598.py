from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

deduped = (
    covered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_policy = (
    deduped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.product").alias("policy_product"))
)

annotated = (
    joined_policy
    .select(
        "*",
        F.when(F.col("product").isin("AUTO", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.when(F.col("claim_type").isin("fire", "flood"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
        F.concat_ws("-", F.col("claim_type"), F.col("policy_product")).alias("claim_type_policy_product_label"),
    )
)

result = (
    annotated
    .groupingSets(
        [["policy_product", "claim_type"], ["policy_product"]],
        "policy_product", "claim_type",
    )
    .agg(
        F.grouping("policy_product").alias("is_total_policy_product"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.array_sort(F.collect_set("policy_product")).alias("policy_product_list"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "is_total_policy_product",
        "cnt",
        "claim_type_list",
        "policy_product_list",
    )
)
