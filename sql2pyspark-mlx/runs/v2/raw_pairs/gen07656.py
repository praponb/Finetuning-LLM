from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 5]) & (F.col("last_restock") > "2024-01-01"))
    .groupBy(
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 60)
    .orderBy("last_restock_quarter", "last_restock_year")
    .limit(50)
)
