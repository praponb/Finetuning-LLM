from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 7, 9, 10]))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") < 480)
)
