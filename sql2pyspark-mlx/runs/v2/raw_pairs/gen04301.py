from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") >= 210)
    .orderBy(F.col("store_id").desc())
)
