from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price").between(130, 140) & (F.col("unit_price") > 190)) | (F.col("quantity") <= 8))
    .select(
        "id",
        "unit_price",
        F.round("unit_price", 0).alias("unit_price_rounded"),
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
    )
)
