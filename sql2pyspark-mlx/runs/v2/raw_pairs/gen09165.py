from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]) & (F.col("last_restock") >= "2024-04-28"))
)

product_id_summary = (
    recent_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = product_id_summary
