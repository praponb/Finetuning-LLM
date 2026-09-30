from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("max_quantity") < 230)
    .select("product_id", "max_quantity", "unique_products")
    .orderBy("unique_products")
)
