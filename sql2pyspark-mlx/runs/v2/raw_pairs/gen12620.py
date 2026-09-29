from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 205) & F.col("product_id").isin([3, 4]))
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy("quantity_range")
)
