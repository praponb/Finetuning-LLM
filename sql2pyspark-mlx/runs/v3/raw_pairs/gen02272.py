from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
)
