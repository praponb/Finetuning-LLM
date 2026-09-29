from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-07-01")
    .select(
        "last_restock",
        "store_id",
        "product_id",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
    .orderBy(F.col("product_id").desc())
)
