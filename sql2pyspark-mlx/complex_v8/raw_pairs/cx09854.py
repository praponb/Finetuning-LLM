from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

stats = (
    scoped_policys
    .groupBy("product", "holder_id")
    .agg(
        F.min("premium").alias("min_premium"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.sum(F.when(F.col("product") == "TRAVEL", 1).otherwise(0)).alias("n_product_travel"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

detail_rows = (
    stats.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isNotNull()), "left")
    .select(
        "s.product",
        "s.holder_id",
        "s.cnt",
        "s.n_product_travel",
        "s.distinct_ids",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    detail_rows
    .filter(F.col("policy_start_date") < F.lit("2025-05-28").cast("date"))
    .select(
        "product",
        "cnt",
        "n_product_travel",
        "distinct_ids",
        "policy_product",
        "policy_status",
    )
    .orderBy("policy_status", F.col("policy_product").desc())
)
