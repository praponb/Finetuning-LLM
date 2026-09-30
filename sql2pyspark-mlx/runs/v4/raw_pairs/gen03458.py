from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    lines
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "tag",
        F.when(F.col("tag").rlike("^(WELCOME5|VIP)$"), "GROUP_A").when(F.col("tag").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("tag_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("tag").desc())
)
