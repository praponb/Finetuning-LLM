from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo").orderBy("price")
w2 = Window.partitionBy("customer_id", "promo")
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc())
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("promo")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.stddev_samp("product_id").over(w2).alias("customer_id_stddev_samp_product_id"),
        F.percent_rank().over(w3).alias("qty_percent_rank"),
    )
)

scored = (
    metrics
    .withColumn("customer_id_stddev_samp_product_id_count_2h", F.count("customer_id_stddev_samp_product_id").over(w4))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w5)), 4))
)

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "price_dense_rank",
        "customer_id_stddev_samp_product_id",
        F.when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "GROUP_A").otherwise("GROUP_B").alias("promo_zone"),
        F.col("max_account_balance"),
    )
)
