from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
        F.when(F.col("quantity") >= 228, "High").when(F.col("quantity") >= 119, "Medium").otherwise("Low").alias("quantity_band"),
    )
    .orderBy("quantity_band", "last_restock_plus_months")
)
