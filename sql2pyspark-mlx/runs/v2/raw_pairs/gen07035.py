from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
        F.when(F.col("quantity") >= 221, "High").when(F.col("quantity") >= 110, "Medium").otherwise("Low").alias("quantity_tier"),
    )
    .orderBy(F.col("quantity_tier").desc())
)
