from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 120)
    .select("product_id", "last_restock_month", "unique_stores", "min_quantity")
)
