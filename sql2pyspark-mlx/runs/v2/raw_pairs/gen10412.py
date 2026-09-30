from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("store_id") == 1) | (F.col("store_id") == 6))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

product_id_summary = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    product_id_summary
    .filter(F.col("unique_stores") < 6)
    .select("product_id", "store_id", "latest_last_restock", "unique_stores")
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(1)
)
