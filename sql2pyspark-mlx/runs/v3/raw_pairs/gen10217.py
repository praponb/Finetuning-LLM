from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 6]))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("avg_quantity", F.col("quantity_count").desc())
)
