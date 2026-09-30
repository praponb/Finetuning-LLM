from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-05-01")
    .withColumn("sale_date_minus_61d", F.date_sub("sale_date", 61))
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.units")).alias("min_units"),
        F.round(F.sum(F.col("s.revenue")), 2).alias("total_revenue"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
