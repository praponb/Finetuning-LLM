from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5]))
    .select(
        "store_id",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
    )
)
