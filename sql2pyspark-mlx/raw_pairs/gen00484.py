from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 4) | (F.year("last_restock") == 2023))
    .select(
        "id",
        "last_restock",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
    )
    .orderBy("id", "last_restock")
)
