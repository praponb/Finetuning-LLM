from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 9, 10]))
    .withColumn("units_tier", F.when(F.col("units") >= 34, "High").when(F.col("units") >= 11, "Medium").otherwise("Low"))
    .withColumn("units_whole", F.ceil("units"))
)

result = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.units_whole")), 2).alias("total_units_whole"),
        F.sum(F.col("s.revenue")).alias("total_revenue"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("total_units_whole")
)
