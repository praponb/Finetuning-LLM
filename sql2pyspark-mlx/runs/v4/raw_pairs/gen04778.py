from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

result = (
    valid
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id") == 1)
        .select("id")
    )
)
