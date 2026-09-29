from pyspark.sql import functions as F

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").like("%Office%")), "left_anti")
)

result = (
    eligible
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.5, "LARGE").otherwise("SMALL"))
    .rollup("rating_tier")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("rating_per_chiang_mai"),
        F.grouping("rating_tier").alias("gid"),
    )
    .orderBy(F.col("rating_tier").asc_nulls_first())
)
