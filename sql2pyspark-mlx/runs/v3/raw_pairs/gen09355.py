from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "quantity",
        "product_id",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_scaled"),
    )
    .orderBy(F.col("quantity").desc())
)
