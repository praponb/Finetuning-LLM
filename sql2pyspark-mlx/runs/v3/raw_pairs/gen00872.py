from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
