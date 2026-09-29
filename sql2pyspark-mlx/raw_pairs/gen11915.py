from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    lines
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_count"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.size(F.collect_set("tag")).alias("num_tags"),
    )
    .orderBy(F.col("bundle_count").desc())
)
