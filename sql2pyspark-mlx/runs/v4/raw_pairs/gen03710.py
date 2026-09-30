from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

flattened = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    flattened
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "product_id",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("max_account_balance").desc_nulls_first(), "promo")
)
