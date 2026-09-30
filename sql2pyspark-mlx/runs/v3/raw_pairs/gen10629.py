from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        F.round(F.stddev(F.col("store_sales.units")), 2).alias("stddev_units"),
        F.max(F.col("store_sales.sale_date")).alias("last_sale_date"),
    )
    .orderBy(F.col("stddev_units").desc(), "region")
    .limit(10)
)
