from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .cube(
        F.when(F.col("price") >= 245, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.max("qty").alias("max_qty"),
        F.round(F.sum("pos") / F.nullif(F.count(F.when(F.col("promo") == "VIP", 1)), F.lit(0)), 2).alias("pos_per_vip"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
        F.count("*").alias("num_rows"),
    )
)
