from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .groupBy(
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
