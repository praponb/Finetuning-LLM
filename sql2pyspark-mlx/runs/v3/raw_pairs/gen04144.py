from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        "last_restock",
        "store_id",
        F.ceil("quantity").alias("quantity_whole"),
        F.floor("quantity").alias("quantity_whole_2"),
    )
    .orderBy(F.col("quantity_whole_2").desc(), F.col("quantity_whole").desc())
)
