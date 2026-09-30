from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

result = (
    lines
    .groupingSets([["tag", "customer_id"], ["tag"]], "tag", "customer_id")
    .agg(
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events"),
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events_2"),
        F.count("*").alias("event_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("tag")).alias("num_tags_2"),
        F.grouping("tag").alias("gid"),
    )
)
