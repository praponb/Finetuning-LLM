from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

result = (
    exploded
    .groupBy("code")
    .agg(
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("first_created_at"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
    )
    .filter(F.col("vip_rows") < 1000)
    .orderBy("num_codes")
)
