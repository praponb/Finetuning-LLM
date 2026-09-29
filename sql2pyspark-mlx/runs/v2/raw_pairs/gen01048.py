from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.store_id").isin([1, 3, 4, 5]))
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
)
