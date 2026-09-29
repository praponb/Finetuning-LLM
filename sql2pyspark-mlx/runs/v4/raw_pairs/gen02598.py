from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.when(F.col("quantity") >= 242, "High").when(F.col("quantity") >= 92, "Medium").otherwise("Low").alias("quantity_band"),
        F.col("quantity").cast("string").alias("quantity_num"),
    )
    .orderBy("quantity_band", "id")
    .limit(20)
)
