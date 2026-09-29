from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units") != 19)
    .groupBy(
        F.col("ss.product_id"),
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .filter(F.col("min_quantity") >= 100)
    .orderBy(
        F.col("stddev_quantity").desc(),
        F.col("product_id").desc(),
        "sale_quarter",
    )
    .limit(1)
)
