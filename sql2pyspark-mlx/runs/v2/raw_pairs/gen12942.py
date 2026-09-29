from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.month("last_restock").isin([5, 6, 8]) | F.col("product_id").isin([4, 8, 9])))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)

product_id_summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    product_id_summary
    .filter(F.col("unique_products") > 10)
    .select("product_id", "unique_products")
)
