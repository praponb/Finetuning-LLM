from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

result = (
    lines
    .groupBy("customer_id", "tag")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_count"),
    )
)
