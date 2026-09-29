from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.year("last_restock").alias("last_restock_year"),
        "store_id",
        "product_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("avg_quantity") >= 65)
)
