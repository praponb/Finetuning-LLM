from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("quantity_range") >= 48)
    .select("product_id", "store_id", "quantity_range", "last_last_restock")
    .orderBy("store_id")
)
