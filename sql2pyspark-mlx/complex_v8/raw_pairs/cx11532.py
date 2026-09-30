from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug_2"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("end_date").alias("last_policy_end_date"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("policy_status_count"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("policy_product_count"),
    )
)

with_counts = (
    filtered_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.last_policy_end_date",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isNotNull()), "left_anti")
)

result = (
    with_activity
    .groupingSets(
        [["holder_id", "product", "policy_product_count"], ["holder_id"]],
        "holder_id", "product", "policy_product_count",
    )
    .agg(F.array_sort(F.collect_set("product")).alias("product_list"))
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("policy_product_count").asc_nulls_last(),
    )
)
