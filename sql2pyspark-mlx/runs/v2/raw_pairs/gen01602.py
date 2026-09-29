from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 232) & (F.year("last_restock") >= 2024) & (F.col("last_restock") <= "2023-10-21"))
    .groupBy(
        "store_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
        "product_id",
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
