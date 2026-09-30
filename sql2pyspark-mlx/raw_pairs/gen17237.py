from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "k", "attr_value", F.col("min_account_balance"))
    .orderBy(F.col("k").desc_nulls_first())
)
