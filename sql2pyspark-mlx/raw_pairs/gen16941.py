from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("attributes").alias("key", "v"))

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.count_if(F.col("key") == "gift").alias("gift_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_rows"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_keys") >= 1)
)
