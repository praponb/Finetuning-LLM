from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("Unknown")))
)

agg = (
    valid_products
    .groupBy(
        F.when(F.col("cost") >= 780, "high").when(F.col("cost") >= 170, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    agg
    .orderBy(F.col("cost_bucket").desc())
    .limit(5)
)
