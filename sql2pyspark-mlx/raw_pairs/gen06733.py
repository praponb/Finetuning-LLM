from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2024-10-21") & F.col("product_id").isin([2, 3, 8, 10]))
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
