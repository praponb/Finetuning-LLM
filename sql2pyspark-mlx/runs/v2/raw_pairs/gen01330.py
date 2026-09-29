from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]) & (F.col("product_id") == 5) & (F.col("product_id") == 3))
)

agg = (
    src
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    agg
    .filter(F.col("quantity_range") >= 43)
    .orderBy(F.col("avg_quantity").desc(), "last_restock_month")
    .limit(5)
)
