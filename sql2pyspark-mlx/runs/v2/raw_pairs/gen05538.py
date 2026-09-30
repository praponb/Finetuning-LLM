from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 220)
    .drop("max_quantity")
)
