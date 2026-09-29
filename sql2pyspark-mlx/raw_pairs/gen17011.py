from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("price") >= 190, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("promo") == "SPRING10", 1)), F.lit(0)), 2).alias("qty_per_spring10"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
    .filter(F.col("avg_qty") > 0)
)
