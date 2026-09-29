from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .groupBy("promo")
    .agg(
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("qty_per_bundle"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count_if(F.col("promo") == "BUNDLE").alias("bundle_events"),
    )
    .filter(F.col("qty_per_bundle") > 0)
    .orderBy(F.col("qty_per_bundle").desc())
)
