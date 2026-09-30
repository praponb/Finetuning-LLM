from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").between(73, 145) & F.col("product_id").isin([4, 7, 10])) | (F.col("quantity") < 67))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)
