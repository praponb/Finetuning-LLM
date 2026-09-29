from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([14, 38]))
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_scaled"),
    )
)
