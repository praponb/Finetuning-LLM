from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    exploded
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "product_id", "qty", F.col("max_account_balance"))
    .orderBy(F.col("id").desc(), "max_account_balance")
)
