from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.year(F.col("ss.sale_date")).alias("sale_year"),
        F.when(F.col("ss.units") >= 45, "high").when(F.col("ss.units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
