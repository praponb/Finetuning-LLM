from pyspark.sql import functions as F

src = df_inventory.filter(F.col("store_id") == 2)

agg = (
    src
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = agg.filter(F.col("quantity_range") < 23)
