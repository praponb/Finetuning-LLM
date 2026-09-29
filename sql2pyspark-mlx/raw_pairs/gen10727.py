from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.max("last_restock").alias("last_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 170)
    .drop("max_quantity")
)
