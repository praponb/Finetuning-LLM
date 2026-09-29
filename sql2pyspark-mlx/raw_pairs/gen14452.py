from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"), "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("avg_quantity") > 150)
    .select("last_restock_year", "store_id", "avg_quantity", "min_last_restock")
)
