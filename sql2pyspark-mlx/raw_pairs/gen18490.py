from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .rollup(
        F.when(F.col("price") >= 239, "LARGE").otherwise("SMALL").alias("price_tier"),
        "promo",
    )
    .agg(
        F.sum("price").alias("total_price"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
    )
    .orderBy("price_tier")
)
