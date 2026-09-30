from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("store_sales.product_id") == 6)
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.round(F.stddev(F.col("store_sales.units")), 2).alias("stddev_units"),
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("stddev_units").desc())
)
