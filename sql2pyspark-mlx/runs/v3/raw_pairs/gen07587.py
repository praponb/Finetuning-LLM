from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("store_id").isin([1, 3]))

summary = (
    valid_inventory
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    summary
    .filter(F.col("total_quantity") >= 460)
    .orderBy(F.col("product_id").desc(), F.col("total_quantity").desc())
)
