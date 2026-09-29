from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 68)
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("unique_stores") > 8)
    .select("store_id", "unique_stores", "total_quantity")
)
