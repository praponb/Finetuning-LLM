from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .cube(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.revenue")).alias("total_revenue"),
        F.grouping_id().alias("grouping_level"),
    )
)
