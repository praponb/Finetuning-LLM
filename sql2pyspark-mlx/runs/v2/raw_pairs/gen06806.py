from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 153)
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("quantity_count").desc())
)
