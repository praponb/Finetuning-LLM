from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 5) & (F.col("quantity") < 3))
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.col("quantity").cast("double").alias("quantity_str"),
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)
