from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.countDistinct("code").alias("distinct_codes"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_rows"),
    )
    .orderBy("vip_rows")
)
