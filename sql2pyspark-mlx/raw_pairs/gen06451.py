from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "product_id", "last_restock_month")
    .limit(3)
)
