from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    exploded
    .select(
        "id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(WELCOME5|VIP)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(F.col("promo_zone").desc())
)
