from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        (F.col("pos") + 1).alias("item_no"),
        F.col("line.product_id"),
        F.col("line.price"),
    )
)

result = (
    exploded
    .cube(
        F.when(F.col("product_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
        F.when(F.col("item_no") >= 3, "LARGE").otherwise("SMALL").alias("item_no_tier"),
    )
    .agg(
        F.percentile_approx("item_no", 0.95).alias("item_no_p95"),
        F.round(F.avg("item_no"), 2).alias("avg_item_no"),
        F.count("*").alias("event_count"),
        F.min("price").alias("min_price"),
        F.percentile_approx("item_no", 0.5).alias("item_no_p50"),
        F.round(F.avg("item_no"), 2).alias("avg_item_no_2"),
    )
)
