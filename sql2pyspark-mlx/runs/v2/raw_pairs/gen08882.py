from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.when(F.col("quantity") >= 257, "High").when(F.col("quantity") >= 53, "Medium").otherwise("Low").alias("quantity_band"),
    )
)
