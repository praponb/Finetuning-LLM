from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "store_id",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)
