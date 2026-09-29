from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("total_quantity") > 670)
)
