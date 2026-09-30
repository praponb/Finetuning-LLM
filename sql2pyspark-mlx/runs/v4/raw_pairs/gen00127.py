from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-07-15")
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 51)
    .select("product_id", "total_quantity", "stddev_quantity")
    .orderBy("product_id")
)
