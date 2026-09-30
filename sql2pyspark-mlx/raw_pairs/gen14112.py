from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 230) & F.col("product_id").isNotNull())
    .select(
        "id",
        "unit_price",
        "quantity",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_level"),
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_adjusted"),
    )
)
