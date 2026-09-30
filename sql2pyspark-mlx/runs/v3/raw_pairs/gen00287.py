from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        "product_id",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
    .orderBy(F.col("quantity_str").desc(), "id")
    .limit(20)
)
