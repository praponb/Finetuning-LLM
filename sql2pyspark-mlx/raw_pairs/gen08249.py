from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .cube(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.revenue")).alias("total_revenue"),
        F.min(F.col("e.units")).alias("min_units"),
    )
    .orderBy(F.col("total_revenue").asc_nulls_first())
)
