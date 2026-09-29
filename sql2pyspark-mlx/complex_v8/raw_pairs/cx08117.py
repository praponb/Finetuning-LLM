from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("status").isin("EXPIRED", "LAPSED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3544.1, "large").when(F.col("premium") >= 2289.0, "medium").otherwise("small").alias("premium_tier"),
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

profiled = (
    prepared_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

derived = (
    profiled
    .filter(F.col("product").isin("HEALTH", "AUTO", "HOME"))
    .withColumn("policy_count_tier", F.when(F.col("policy_count") >= 6, "large").when(F.col("policy_count") >= 3, "medium").otherwise("small"))
)

qualified = (
    derived.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("policy_count") > 1)
)

result = (
    qualified
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "premium_tier",
        "status_product_label",
        "policy_count",
        "policy_count_tier",
    )
    .orderBy(F.col("holder_id").asc(), F.col("premium_tier").asc(), "id")
    .limit(5)
)
