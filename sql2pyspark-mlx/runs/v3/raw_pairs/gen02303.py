from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 8]))
    .select(
        "id",
        "product_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)
