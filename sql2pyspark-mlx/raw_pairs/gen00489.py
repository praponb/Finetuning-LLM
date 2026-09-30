from pyspark.sql import functions as F

base = df_products.filter(F.col("price") >= 1140)

brand_summary = (
    base
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    brand_summary
    .filter(F.col("fashion_count") > 46)
    .orderBy(F.col("stddev_cost").desc())
)
