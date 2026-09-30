from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("quantity_range") > 68)
    .select("last_restock_month", "quantity_range", "total_count")
    .orderBy("last_restock_month")
)
