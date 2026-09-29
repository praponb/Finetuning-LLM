from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        "store_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy(F.col("last_restock_month").desc())
)
