from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("unique_products") > 7)
    .select("product_id", "unique_products", "avg_quantity")
    .orderBy("product_id")
)
