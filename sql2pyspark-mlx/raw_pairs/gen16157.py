from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 910)
    .orderBy("last_restock_month")
    .limit(1)
)
