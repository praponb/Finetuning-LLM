from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)
