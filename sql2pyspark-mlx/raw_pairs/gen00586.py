from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 66)
    .select("store_id", "last_restock_month", "last_last_restock", "avg_quantity")
    .orderBy("last_restock_month")
)
