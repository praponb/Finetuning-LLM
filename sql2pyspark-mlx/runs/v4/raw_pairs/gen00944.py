from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 109)
    .groupBy(
        F.when(F.col("quantity") >= 247, "high").when(F.col("quantity") >= 119, "mid").otherwise("low").alias("quantity_bucket"),
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy("quantity_bucket", "last_restock_year")
    .limit(20)
)
