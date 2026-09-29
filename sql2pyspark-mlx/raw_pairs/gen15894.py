from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

result = (
    flattened
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"), "code")
    .agg(
        F.count_if(F.col("code") == "VIP").alias("vip_events"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count(F.when(F.col("code") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy("activity_day")
)
