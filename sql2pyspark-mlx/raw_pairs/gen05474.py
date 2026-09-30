from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("pos") >= 2, "PREMIUM").otherwise("STANDARD").alias("pos_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
)
