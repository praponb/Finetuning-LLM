from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("row_count") >= 4)
    .orderBy("last_restock_month")
    .limit(20)
)
