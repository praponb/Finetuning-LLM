from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 217)
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)
