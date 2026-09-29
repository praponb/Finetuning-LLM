from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .orderBy("max_quantity")
)
