from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("product_id") >= 11, "HIGH").otherwise("LOW").alias("product_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_count"),
    )
    .orderBy(F.col("product_id_tier").desc_nulls_last())
)
