from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-03-01", "2025-06-01"))
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(df_store_sales.select(F.col("sale_date").alias("event_date")))
    .distinct()
)
