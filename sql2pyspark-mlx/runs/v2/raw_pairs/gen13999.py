from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("max_quantity") > 120)
)
