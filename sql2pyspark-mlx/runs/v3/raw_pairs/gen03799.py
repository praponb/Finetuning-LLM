from pyspark.sql import functions as F

src = df_inventory.filter(F.col("product_id").isin([4, 7]) & (F.col("quantity") >= 191))

inventory_stats = (
    src
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    inventory_stats
    .select("store_id", "quantity_range")
    .orderBy(F.col("quantity_range").desc(), "store_id")
)
