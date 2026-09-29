from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("last_restock") <= "2023-06-11") & F.col("store_id").isin([2, 5])) | (F.col("quantity") >= 70))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 220)
    .drop("avg_quantity")
    .orderBy("quantity_range", "store_id")
    .limit(10)
)
