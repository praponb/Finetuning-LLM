from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([5, 8, 12]))
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        F.year("last_restock").alias("last_restock_year"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
