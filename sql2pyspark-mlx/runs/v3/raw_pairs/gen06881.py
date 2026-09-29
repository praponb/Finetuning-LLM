from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("quantity") <= 72)

product_id_summary = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    product_id_summary
    .filter(F.col("row_count") > 7)
    .orderBy(F.col("row_count").desc())
)
