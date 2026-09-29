from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select(
        "quantity",
        "store_id",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.when(F.col("quantity") >= 251, "High").when(F.col("quantity") >= 87, "Medium").otherwise("Low").alias("quantity_band"),
    )
)
