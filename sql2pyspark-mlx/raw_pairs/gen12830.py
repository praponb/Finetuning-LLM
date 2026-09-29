from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5]) & (F.col("store_id").isin([2, 3, 5, 6]) | (F.col("last_restock") <= "2023-05-02")))
)

agg = (
    base
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    agg
    .filter(F.col("avg_quantity") >= 170)
    .select("product_id", "last_restock_month", "min_last_restock", "avg_quantity")
    .orderBy("last_restock_month")
)
