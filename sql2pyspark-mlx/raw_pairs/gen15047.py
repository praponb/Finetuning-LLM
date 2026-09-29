from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(WELCOME5|VIP)$"), "GROUP_A").otherwise("GROUP_B").alias("promo_zone"),
    )
)
