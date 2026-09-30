from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.count("*").alias("num_inventory"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 1)
    .orderBy(
        F.col("quantity_count").desc(),
        F.col("num_inventory").desc(),
        "store_id",
        "last_restock_month",
    )
    .limit(10)
)
