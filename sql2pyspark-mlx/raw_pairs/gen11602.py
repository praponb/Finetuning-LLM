from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("store_id") == 6) & (F.col("store_id") == 2)) | F.col("product_id").isin([2, 4, 6, 8]))
    .groupBy(
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("last_restock_quarter")
)
