from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

result = (
    exploded
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max("created_at").alias("last_seen"),
        F.count(F.when(F.col("code") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("code")).alias("num_codes"),
    )
)
