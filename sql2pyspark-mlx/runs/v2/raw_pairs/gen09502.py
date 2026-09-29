from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("order_id") == 12) & F.col("order_id").isin([22, 40])) | (F.col("quantity") > 7))
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
        F.when(F.col("unit_price") >= 400, "High").when(F.col("unit_price") >= 150, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)
