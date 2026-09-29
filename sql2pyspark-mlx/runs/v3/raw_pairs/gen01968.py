from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 2) & F.col("quantity").between(63, 151))
    .groupBy(
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
)
