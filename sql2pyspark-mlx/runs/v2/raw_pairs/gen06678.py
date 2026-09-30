from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 3, 4, 6]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .orderBy(F.col("brand").desc())
)
