from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter((F.col("quantity") != 134) & (F.col("quantity") > 206))

product_id_summary = (
    filtered_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    product_id_summary
    .select("product_id", "store_id", "unique_products", "avg_quantity")
    .orderBy(F.col("store_id").desc())
)
