from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 3) & F.col("product_id").isin([2, 4, 6, 7]) & F.col("quantity").between(66, 121))
    .cube("store_id", F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy(F.col("total_quantity").desc(), "last_restock_month")
)
