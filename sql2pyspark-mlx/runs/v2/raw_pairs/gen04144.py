from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 176)
    .groupBy(
        F.year("last_restock").alias("last_restock_year"),
        "product_id",
        "store_id",
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
)
