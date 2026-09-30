from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity") >= 69) | (F.col("quantity") >= 119)))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)
