from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_semi")
)

result = (
    kept
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.6, "HIGH").otherwise("LOW"))
    .groupingSets([["city", "rating_tier"], ["city"]], "city", "rating_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
    )
)
