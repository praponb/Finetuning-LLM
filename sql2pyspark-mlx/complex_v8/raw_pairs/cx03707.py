from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_all_by_premium_prior_2 = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(-2, -1)

joined_ranges = (
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

unique_rows = (
    joined_ranges
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "adjuster_id",
        "product",
        "premium",
        F.size(F.collect_set("status").over(w_policy_id_adjuster_id)).alias("distinct_status_in_policy_id_adjuster_id"),
        F.sum("premium").over(w_all_by_premium_prior_2).alias("premium_sum_2_prior"),
        F.size(F.collect_set("product").over(w_policy_id_adjuster_id)).alias("distinct_product_in_policy_id_adjuster_id"),
    )
)

result = (
    windowed
    .rollup("adjuster_id", "policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("n_product_is_not_null"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
