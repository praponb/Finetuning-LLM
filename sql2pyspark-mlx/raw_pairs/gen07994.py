from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .rollup("promo", "customer_id")
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("qty"))).alias("bundle_total_qty"),
    )
    .orderBy(F.col("promo").desc())
)
