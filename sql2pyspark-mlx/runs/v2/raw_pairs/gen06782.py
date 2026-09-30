from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("store_sales.store_id").isin([1, 4, 6]))
    .groupBy(
        F.col("stores.region"),
        F.quarter(F.col("stores.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.round(F.stddev(F.col("store_sales.units")), 2).alias("stddev_units"),
        F.min(F.col("store_sales.revenue")).alias("min_revenue"),
    )
    .filter(F.col("min_revenue") > 2300)
)
