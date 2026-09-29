from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"), "product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 3)
    .drop("quantity_count")
)
