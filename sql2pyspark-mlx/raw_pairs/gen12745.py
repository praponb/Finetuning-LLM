from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-03-01")
    .select(
        "id",
        "quantity",
        "product_id",
        "store_id",
        F.when(F.col("quantity") >= 245, "High").when(F.col("quantity") >= 90, "Medium").otherwise("Low").alias("quantity_tier"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
