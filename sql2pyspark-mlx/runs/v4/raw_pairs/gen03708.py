from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    lines
    .groupBy("tag")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.min("created_at").alias("first_seen"),
        F.min("created_at").alias("first_seen_2"),
    )
    .filter(F.col("distinct_ids") > 1)
)
