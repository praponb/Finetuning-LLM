from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("product_id").isin([2, 5, 6, 7]) & F.col("product_id").isin([1, 3, 7])) | (F.year("last_restock") >= 2023))
)

summary = (
    valid_inventory
    .groupBy(
        F.when(F.col("quantity") >= 264, "high").when(F.col("quantity") >= 30, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    summary
    .filter(F.col("total_quantity") >= 470)
    .orderBy(F.col("quantity_bucket").desc(), "total_quantity")
)
