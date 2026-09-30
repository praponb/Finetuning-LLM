from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.units").between(12, 13))
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .orderBy("stddev_units")
)
