from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 7, 8]))
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 170)
)
