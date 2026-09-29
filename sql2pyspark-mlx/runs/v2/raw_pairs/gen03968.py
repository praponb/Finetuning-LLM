from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 120)
    .drop("avg_quantity")
)
