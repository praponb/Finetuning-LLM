from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        F.when(F.col("quantity") >= 269, "High").when(F.col("quantity") >= 42, "Medium").otherwise("Low").alias("quantity_band"),
    )
)
