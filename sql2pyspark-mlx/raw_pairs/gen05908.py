from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

filtered = (
    flattened
    .filter(F.col("customer_id").isNotNull() & (F.col("code").eqNullSafe(F.lit("SPRING10"))))
    .select(
        "*",
        F.when(F.col("code").isin(["WELCOME5", "BUNDLE"]), "Group A").otherwise("Other").alias("code_group"),
    )
)

base = (
    filtered
    .filter(F.col("code").eqNullSafe(F.lit("SPRING10")))
    .withColumn("is_vip", F.when(F.col("code") == "VIP", 1).otherwise(0))
)

filtered_carts = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("is_vip") * 0.85), 2).alias("adjusted_is_vip"),
        F.coalesce("is_vip", F.lit(0)).alias("is_vip_filled"),
    )
)

grouped = (
    filtered_carts
    .groupBy("customer_id", "code")
    .agg(
        F.count_if(F.col("code") == "VIP").alias("vip_events"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.round(F.sum("adjusted_is_vip") / F.nullif(F.count(F.when(F.col("code") == "VIP", 1)), F.lit(0)), 2).alias("adjusted_is_vip_per_vip"),
    )
)

result = (
    grouped
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "code",
        "vip_events",
        "num_codes",
        F.when(F.col("code").rlike("^(SPRING10|VIP)$"), "EXTENDED").when(F.col("code").rlike("^(BUNDLE|WELCOME5)$"), "CORE").otherwise("GROUP_A").alias("code_zone"),
        F.col("max_order_amount"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first(), F.col("vip_events").desc())
)
