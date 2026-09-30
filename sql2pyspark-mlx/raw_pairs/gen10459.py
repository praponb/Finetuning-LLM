from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
    )
    .orderBy(F.col("corr_quantity_unit_price").desc_nulls_first())
)
