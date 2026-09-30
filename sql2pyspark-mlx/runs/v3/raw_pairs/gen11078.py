from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(((F.col("quantity") != 190) & (F.col("last_restock") <= "2024-11-10")) | F.col("product_id").isin([1, 2]))
)

last_restock_month_summary = (
    base
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    last_restock_month_summary
    .select("last_restock_month", "total_quantity", "avg_quantity")
)
