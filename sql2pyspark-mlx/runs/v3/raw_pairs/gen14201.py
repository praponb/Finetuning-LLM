from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
