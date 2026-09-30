from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]) & (F.col("quantity") == 238) & (F.year("last_restock") >= 2023))
    .select(
        "id",
        "quantity",
        "product_id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
    .orderBy(F.col("product_id").desc())
)
