from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .orderBy(F.col("avg_units").desc())
)
