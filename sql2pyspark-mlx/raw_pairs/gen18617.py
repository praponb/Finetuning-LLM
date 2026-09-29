from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

result = (
    lines
    .cube("customer_id", "k")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("k").alias("distinct_ks"),
    )
    .orderBy(F.col("num_ks").asc_nulls_first())
)
