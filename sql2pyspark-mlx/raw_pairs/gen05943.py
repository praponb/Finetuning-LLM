from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 290) & (F.col("quantity") < 3))
    .select(
        "id",
        "quantity",
        "order_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 60, "Medium").otherwise("Low").alias("unit_price_band"),
    )
    .orderBy(F.col("id").desc())
)
