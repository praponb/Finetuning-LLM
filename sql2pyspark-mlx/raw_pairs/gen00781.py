from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([7, 9, 10]))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
        F.sum("quantity").alias("total_quantity"),
    )
)
