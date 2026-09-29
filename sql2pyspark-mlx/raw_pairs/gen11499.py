from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5, 6]))
    .select(
        "id",
        "quantity",
        F.when(F.col("quantity") >= 205, "High").when(F.col("quantity") >= 109, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)
