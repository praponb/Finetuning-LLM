from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")

flattened = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

flattened_carts = flattened.select("product_id", "promo", F.explode("coupon_codes").alias("tag"))

base = (
    flattened_carts
    .filter(~F.col("promo").eqNullSafe(F.lit("SPRING10")))
    .withColumn("product_id_scaled", F.round((F.col("product_id") * 0.85), 2))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("NONE")))
)

scored = (
    base
    .filter(F.col("promo").isNull())
    .select(
        "product_id",
        "promo",
        F.min("product_id_scaled").over(w).alias("tag_min_product_id_scaled"),
        F.stddev_samp("product_id").over(w).alias("tag_stddev_samp_product_id"),
        F.max("product_id_scaled").over(w).alias("tag_max_product_id_scaled"),
    )
)

result = (
    scored
    .select(
        "promo",
        "tag_min_product_id_scaled",
        F.when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("promo_zone"),
    )
    .orderBy(F.col("promo_zone").desc_nulls_first())
)
