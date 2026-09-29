from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(~F.col("s.region").isin(["East", "North"]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
)
