from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
    .orderBy(F.col("product_id").desc(), F.col("quantity_rounded").desc())
)
