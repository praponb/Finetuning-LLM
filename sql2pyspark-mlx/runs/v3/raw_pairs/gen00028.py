from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .orderBy(F.col("min_last_restock").desc(), F.col("total_quantity").desc())
)
