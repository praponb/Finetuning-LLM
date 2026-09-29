from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 188, "high").when(F.col("quantity") >= 43, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("unique_products") > 9)
    .select("quantity_bucket", "store_id", "unique_products", "total_count")
)
