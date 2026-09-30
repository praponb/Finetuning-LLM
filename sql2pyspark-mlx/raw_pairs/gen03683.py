from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    flattened
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["code", "customer_id", "activity_day"], ["code", "customer_id"], ["code"]],
        "code",
        "customer_id",
        "activity_day",
    )
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
        F.grouping("code").alias("grouping_level"),
    )
)
