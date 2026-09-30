from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) & F.col("store_id").isin([1, 4, 5, 6]) & (F.col("quantity") < 141))
    .groupBy(F.year("last_restock").alias("last_restock_year"), "product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .filter(F.col("unique_stores") < 3)
)
