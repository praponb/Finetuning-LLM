from pyspark.sql import functions as F

src = df_inventory.filter((F.col("quantity") < 190) & F.col("product_id").isin([8, 10]))

inventory_stats = (
    src
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    inventory_stats
    .select("product_id", "unique_products", "unique_stores")
    .orderBy(F.col("unique_stores").desc(), "product_id")
    .limit(20)
)
