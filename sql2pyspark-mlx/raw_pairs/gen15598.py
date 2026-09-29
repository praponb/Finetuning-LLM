from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

result = (
    flattened
    .groupBy("customer_id", "k")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
    )
    .orderBy(F.col("utm_source_rows").asc_nulls_first())
)
