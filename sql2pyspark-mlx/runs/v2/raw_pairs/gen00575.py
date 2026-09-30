from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("product_id").isin([4, 9]))

agg = (
    filtered_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    agg
    .select("product_id", "first_last_restock", "unique_stores")
    .orderBy(F.col("unique_stores").desc(), "product_id")
    .limit(3)
)
