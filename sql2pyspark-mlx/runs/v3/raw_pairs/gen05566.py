from pyspark.sql import functions as F

src = df_inventory.filter(F.col("last_restock") < "2023-05-01")

summary = (
    src
    .groupBy(F.year("last_restock").alias("last_restock_year"), "product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    summary
    .filter(F.col("unique_products") > 9)
    .select("last_restock_year", "product_id", "unique_stores", "unique_products")
)
