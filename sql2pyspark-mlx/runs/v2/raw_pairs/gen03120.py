from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 70) & (F.col("product_id") == 5))
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("last_restock_month").desc())
    .limit(10)
)
