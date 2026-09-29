from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").isNull())
    .select(
        "id",
        "quantity",
        "product_id",
        "order_id",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 5, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy("order_id")
)
