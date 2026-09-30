from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        "product_id",
        "store_id",
        F.when(F.col("quantity") >= 230, "High").when(F.col("quantity") >= 38, "Medium").otherwise("Low").alias("quantity_tier"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
    .orderBy(F.col("product_id").desc())
)
