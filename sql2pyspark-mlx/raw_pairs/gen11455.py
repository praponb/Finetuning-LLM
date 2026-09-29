from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

result = (
    lines
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets([["code", "activity_day"], ["code"], []], "code", "activity_day")
    .agg(
        F.count(F.when(F.col("code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.max("created_at").alias("last_seen"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
    )
    .filter(F.col("vip_count") < 1000)
    .orderBy(F.col("activity_day").asc_nulls_first())
)
