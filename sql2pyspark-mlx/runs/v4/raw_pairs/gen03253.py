from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    lines
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "OTHER_ZONE").when(F.col("promo").rlike("^(SPRING10|VIP)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_zone"),
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("product_id").asc_nulls_last())
)
