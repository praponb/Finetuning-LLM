from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)
